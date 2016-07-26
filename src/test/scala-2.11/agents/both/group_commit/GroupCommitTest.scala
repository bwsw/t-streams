package agents.both.group_commit

import java.net.InetSocketAddress

import com.aerospike.client.Host
import com.bwsw.tstreams.agents.consumer.{BasicConsumer, BasicConsumerOptions}
import com.bwsw.tstreams.agents.consumer.Offsets.Oldest
import com.bwsw.tstreams.agents.group.CheckpointGroup
import com.bwsw.tstreams.agents.producer.{BasicProducer, BasicProducerOptions, ProducerCoordinationOptions, ProducerPolicies}
import com.bwsw.tstreams.agents.producer.InsertionType.SingleElementInsert
import com.bwsw.tstreams.converter.{ArrayByteToStringConverter, StringToArrayByteConverter}
import com.bwsw.tstreams.data.aerospike.{AerospikeStorageFactory, AerospikeStorageOptions}
import com.bwsw.tstreams.coordination.transactions.transport.impl.TcpTransport
import com.bwsw.tstreams.metadata.MetadataStorageFactory
import com.bwsw.tstreams.streams.BasicStream
import com.datastax.driver.core.Cluster
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._


class GroupCommitTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils{

  val randomKeyspace = randomString
  val cluster = Cluster.builder().addContactPoint("localhost").build()
  val session = cluster.connect()
  CassandraHelper.createKeyspace(session, randomKeyspace)
  CassandraHelper.createMetadataTables(session, randomKeyspace)

  val metadataStorageFactory = new MetadataStorageFactory
  val storageFactory = new AerospikeStorageFactory

  val arrayByteToStringConverter = new ArrayByteToStringConverter
  val stringToArrayByteConverter = new StringToArrayByteConverter

  val hosts = List(
    new Host("localhost",3000),
    new Host("localhost",3001),
    new Host("localhost",3002),
    new Host("localhost",3003))
  val aerospikeOptions = new AerospikeStorageOptions("test", hosts)
  
  val metadataStorage = metadataStorageFactory.getInstance(
    cassandraHosts = List(new InetSocketAddress("localhost", 9042)),
    keyspace = randomKeyspace)


  val streamForProducer: BasicStream[Array[Byte]] = new BasicStream[Array[Byte]](
    name = "test_stream",
    partitions = 3,
    metadataStorage = metadataStorage,
    dataStorage = storageFactory.getInstance(aerospikeOptions),
    ttl = 60 * 10,
    description = "some_description")

  val streamForConsumer = new BasicStream[Array[Byte]](
    name = "test_stream",
    partitions = 3,
    metadataStorage = metadataStorage,
    dataStorage = storageFactory.getInstance(aerospikeOptions),
    ttl = 60 * 10,
    description = "some_description")

  val agentSettings = new ProducerCoordinationOptions(
    agentAddress = s"localhost:8000",
    zkHosts = List(new InetSocketAddress("localhost", 2181)),
    zkRootPath = "/unit",
    zkSessionTimeout = 7000,
    isLowPriorityToBeMaster = false,
    transport = new TcpTransport,
    transportTimeout = 5,
    zkConnectionTimeout = 7)

  val producerOptions = new BasicProducerOptions[String](transactionTTL = 6, transactionKeepAliveInterval = 2, RoundRobinPolicyCreator.getRoundRobinPolicy(streamForProducer, List(0,1,2)), SingleElementInsert, LocalGeneratorCreator.getGen(), agentSettings, stringToArrayByteConverter)

  val consumerOptions = new BasicConsumerOptions[Array[Byte], String](
    transactionsPreload = 10,
    dataPreload = 7,
    consumerKeepAliveInterval = 5,
    arrayByteToStringConverter,
    RoundRobinPolicyCreator.getRoundRobinPolicy(streamForConsumer, List(0,1,2)),
    Oldest,
    LocalGeneratorCreator.getGen(),
    useLastOffset = true)

  val producer = new BasicProducer("test_producer", streamForProducer, producerOptions)
  var consumer = new BasicConsumer("test_consumer", streamForConsumer, consumerOptions)
  
  "Group commit" should "checkpoint all AgentsGroup state" in {
    val group = new CheckpointGroup()
    group.add("producer", producer)
    group.add("consumer", consumer)

    val txn = producer.newTransaction(ProducerPolicies.errorIfOpened)
    txn.send("info1")
    txn.checkpoint()
    Thread.sleep(2000)

    //move consumer offsets
    consumer.getTransaction.get

    //open transaction without close
    producer.newTransaction(ProducerPolicies.errorIfOpened).send("info2")

    group.commit()

    val newStreamForConsumer = new BasicStream[Array[Byte]](
      name = "test_stream",
      partitions = 3,
      metadataStorage = metadataStorage,
      dataStorage = storageFactory.getInstance(aerospikeOptions),
      ttl = 60 * 10,
      description = "some_description")
    consumer = new BasicConsumer("test_consumer", newStreamForConsumer, consumerOptions)
    //assert that the second transaction was closed and consumer offsets was moved
    assert(consumer.getTransaction.get.getAll().head == "info2")
  }

  override def afterAll(): Unit = {
    producer.stop()
    removeZkMetadata("/unit")
    session.execute(s"DROP KEYSPACE $randomKeyspace")
    session.close()
    cluster.close()
    metadataStorageFactory.closeFactory()
    storageFactory.closeFactory()
  }
}
