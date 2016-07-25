package agents.producer

import java.net.InetSocketAddress

import com.bwsw.tstreams.agents.consumer.Offsets.Oldest
import com.bwsw.tstreams.agents.consumer.{BasicConsumer, BasicConsumerOptions}
import com.bwsw.tstreams.agents.producer.InsertionType.SingleElementInsert
import com.bwsw.tstreams.agents.producer._
import com.bwsw.tstreams.converter.{ArrayByteToStringConverter, StringToArrayByteConverter}
import com.bwsw.tstreams.data.cassandra.{CassandraStorageFactory, CassandraStorageOptions}
import com.bwsw.tstreams.coordination.transactions.transport.impl.TcpTransport
import com.bwsw.tstreams.metadata.MetadataStorageFactory
import com.bwsw.tstreams.services.BasicStreamService
import com.datastax.driver.core.Cluster
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._


class BasicProducerWithManyOpenedTxnsTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils{

  val randomKeyspace = randomString
  val temporaryCluster = Cluster.builder().addContactPoint("localhost").build()
  val temporarySession = temporaryCluster.connect()
  CassandraHelper.createKeyspace(temporarySession, randomKeyspace)
  CassandraHelper.createMetadataTables(temporarySession, randomKeyspace)
  CassandraHelper.createDataTable(temporarySession, randomKeyspace)

  val metadataStorageFactory = new MetadataStorageFactory
  val storageFactory = new CassandraStorageFactory

  val stringToArrayByteConverter = new StringToArrayByteConverter
  val arrayByteToStringConverter = new ArrayByteToStringConverter

  val cassandraOptions = new CassandraStorageOptions(List(new InetSocketAddress("localhost",9042)), randomKeyspace)

  val stream = BasicStreamService.createStream(
    streamName = "test_stream",
    partitions = 3,
    ttl = 60 * 10,
    description = "unit_testing",
    metadataStorage = metadataStorageFactory.getInstance(List(new InetSocketAddress("localhost", 9042)), randomKeyspace),
    dataStorage = storageFactory.getInstance(cassandraOptions))

  val agentSettings = new ProducerCoordinationOptions(
    agentAddress = s"localhost:8000",
    zkHosts = List(new InetSocketAddress("localhost", 2181)),
    zkRootPath = "/unit",
    zkSessionTimeout = 7000,
    isLowPriorityToBeMaster = false,
    transport = new TcpTransport,
    transportTimeout = 5,
    zkConnectionTimeout = 7)

  val producerOptions = new BasicProducerOptions[String, Array[Byte]](
    transactionTTL = 10,
    transactionKeepAliveInterval = 2,
    producerKeepAliveInterval = 1,
    RoundRobinPolicyCreator.getRoundRobinPolicy(stream, List(0,1,2)),
    SingleElementInsert,
    LocalGeneratorCreator.getGen(),
    agentSettings,
    stringToArrayByteConverter)

  val producer = new BasicProducer("test_producer", stream, producerOptions)

  val metadataStorageInstForConsumer = metadataStorageFactory.getInstance(
    cassandraHosts = List(new InetSocketAddress("localhost", 9042)),
    keyspace = randomKeyspace)

  val cassandraInstForConsumer = storageFactory.getInstance(cassandraOptions)

  val streamForConsumer = BasicStreamService.loadStream[Array[Byte]](
    streamName = "test_stream",
    metadataStorage = metadataStorageInstForConsumer,
    dataStorage = cassandraInstForConsumer)

  val consumerOptions = new BasicConsumerOptions[Array[Byte], String](
    transactionsPreload = 10,
    dataPreload = 7,
    consumerKeepAliveInterval = 5,
    arrayByteToStringConverter,
    RoundRobinPolicyCreator.getRoundRobinPolicy(streamForConsumer, List(0,1,2)),
    Oldest,
    LocalGeneratorCreator.getGen(),
    useLastOffset = true)

  val consumer = new BasicConsumer("test_consumer", streamForConsumer, consumerOptions)

  "BasicProducer.newTransaction()" should "return BasicProducerTransaction instance" in {
    val data1 = (for (i <- 0 until 10) yield randomString).sorted
    val data2 = (for (i <- 0 until 10) yield randomString).sorted
    val data3 = (for (i <- 0 until 10) yield randomString).sorted
    val txn1: BasicProducerTransaction[String, Array[Byte]] = producer.newTransaction(ProducerPolicies.errorIfOpened)
    val txn2: BasicProducerTransaction[String, Array[Byte]] = producer.newTransaction(ProducerPolicies.errorIfOpened)
    val txn3: BasicProducerTransaction[String, Array[Byte]] = producer.newTransaction(ProducerPolicies.errorIfOpened)
    data1.foreach(x=>txn1.send(x))
    data2.foreach(x=>txn2.send(x))
    data3.foreach(x=>txn3.send(x))
    txn3.checkpoint()
    txn2.checkpoint()
    txn1.checkpoint()

    assert(consumer.getTransaction.get.getAll().sorted == data1)
    assert(consumer.getTransaction.get.getAll().sorted == data2)
    assert(consumer.getTransaction.get.getAll().sorted == data3)
    assert(consumer.getTransaction.isEmpty)
  }

  override def afterAll(): Unit = {
    producer.stop()
    removeZkMetadata("/unit")
    temporarySession.execute(s"DROP KEYSPACE $randomKeyspace")
    temporarySession.close()
    temporaryCluster.close()
    metadataStorageFactory.closeFactory()
    storageFactory.closeFactory()
  }
}
