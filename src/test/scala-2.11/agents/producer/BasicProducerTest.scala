package agents.producer

import java.net.InetSocketAddress
import com.bwsw.tstreams.agents.producer.InsertionType.SingleElementInsert
import com.bwsw.tstreams.agents.producer._
import com.bwsw.tstreams.converter.StringToArrayByteConverter
import com.bwsw.tstreams.data.cassandra.{CassandraStorageOptions, CassandraStorageFactory}
import com.bwsw.tstreams.coordination.transactions.transport.impl.TcpTransport
import com.bwsw.tstreams.common.zkservice.ZkService
import com.bwsw.tstreams.metadata.MetadataStorageFactory
import com.bwsw.tstreams.services.BasicStreamService
import com.datastax.driver.core.Cluster
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import testutils._


class BasicProducerTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils{
  val randomKeyspace = randomString
  val temporaryCluster = Cluster.builder().addContactPoint("localhost").build()
  val temporarySession = temporaryCluster.connect()
  CassandraHelper.createKeyspace(temporarySession, randomKeyspace)
  CassandraHelper.createMetadataTables(temporarySession, randomKeyspace)
  CassandraHelper.createDataTable(temporarySession, randomKeyspace)

  val metadataStorageFactory = new MetadataStorageFactory
  val storageFactory = new CassandraStorageFactory

  val stringToArrayByteConverter = new StringToArrayByteConverter

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
    zkTimeout = 7000,
    isLowPriorityToBeMaster = false,
    transport = new TcpTransport,
    transportTimeout = 5)

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

  "BasicProducer.newTransaction()" should "return BasicProducerTransaction instance" in {
    val txn: BasicProducerTransaction[String, Array[Byte]] = producer.newTransaction(ProducerPolicies.errorIfOpen)
    txn.checkpoint()
    txn.isInstanceOf[BasicProducerTransaction[_,_]] shouldEqual true
  }

  "BasicProducer.newTransaction(ProducerPolicies.errorIfOpen)" should "throw exception if previous transaction was not closed" in {
    val txn1: BasicProducerTransaction[String, Array[Byte]] = producer.newTransaction(ProducerPolicies.checkpointIfOpen, 2)
    intercept[IllegalStateException] {
       producer.newTransaction(ProducerPolicies.errorIfOpen, 2)
    }
    txn1.checkpoint()
  }

  "BasicProducer.newTransaction(checkpointIfOpen)" should "not throw exception if previous transaction was not closed" in {
    producer.newTransaction(ProducerPolicies.checkpointIfOpen, 2)
    val txn2 = producer.newTransaction(ProducerPolicies.checkpointIfOpen, 2)
    txn2.checkpoint()
  }

  "BasicProducer.getTransaction()" should "return transaction reference if it was created or None" in {
    val txn = producer.newTransaction(ProducerPolicies.checkpointIfOpen, 1)
    val txnRef = producer.getTransaction(1)
    txn.checkpoint()
    val checkVal = txnRef.get == txn
    checkVal shouldEqual true
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
