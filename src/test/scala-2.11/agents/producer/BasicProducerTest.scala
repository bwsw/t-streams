package agents.producer

import java.net.InetSocketAddress

import com.bwsw.tstreams.agents.producer.InsertionType.SingleElementInsert
import com.bwsw.tstreams.agents.producer._
import com.bwsw.tstreams.coordination.transactions.transport.impl.TcpTransport
import com.bwsw.tstreams.data.cassandra.CassandraStorageOptions
import com.bwsw.tstreams.services.BasicStreamService
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._


class BasicProducerTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  val cassandraOptions = new CassandraStorageOptions(List(new InetSocketAddress("localhost", 9042)), randomKeyspace)

  val stream = BasicStreamService.createStream(
    streamName = "test_stream",
    partitions = 3,
    ttl = 60 * 10,
    description = "unit_testing",
    metadataStorage = metadataStorageFactory.getInstance(List(new InetSocketAddress("localhost", 9042)), randomKeyspace),
    dataStorage = cassandraStorageFactory.getInstance(cassandraOptions))

  val agentSettings = new ProducerCoordinationOptions(
    agentAddress = s"localhost:8000",
    zkHosts = List(new InetSocketAddress("localhost", 2181)),
    zkRootPath = "/unit",
    zkSessionTimeout = 7000,
    isLowPriorityToBeMaster = false,
    transport = new TcpTransport,
    transportTimeout = 5,
    zkConnectionTimeout = 7)

  val producerOptions = new BasicProducerOptions[String](transactionTTL = 10, transactionKeepAliveInterval = 2, RoundRobinPolicyCreator.getRoundRobinPolicy(stream, List(0, 1, 2)), SingleElementInsert, LocalGeneratorCreator.getGen(), agentSettings, stringToArrayByteConverter)

  val producer = new BasicProducer("test_producer", stream, producerOptions)

  "BasicProducer.newTransaction()" should "return BasicProducerTransaction instance" in {
    val txn: BasicProducerTransaction[String] = producer.newTransaction(ProducerPolicies.errorIfOpened)
    txn.checkpoint()
    txn.isInstanceOf[BasicProducerTransaction[_]] shouldEqual true
  }

  "BasicProducer.newTransaction(ProducerPolicies.errorIfOpen)" should "throw exception if previous transaction was not closed" in {
    val txn1: BasicProducerTransaction[String] = producer.newTransaction(ProducerPolicies.checkpointIfOpened, 2)
    intercept[IllegalStateException] {
      producer.newTransaction(ProducerPolicies.errorIfOpened, 2)
    }
    txn1.checkpoint()
  }

  "BasicProducer.newTransaction(checkpointIfOpen)" should "not throw exception if previous transaction was not closed" in {
    producer.newTransaction(ProducerPolicies.checkpointIfOpened, 2)
    val txn2 = producer.newTransaction(ProducerPolicies.checkpointIfOpened, 2)
    txn2.checkpoint()
  }

  "BasicProducer.getTransaction()" should "return transaction reference if it was created or None" in {
    val txn = producer.newTransaction(ProducerPolicies.checkpointIfOpened, 1)
    val txnRef = producer.getOpenTransactionForPartition(1)
    txn.checkpoint()
    val checkVal = txnRef.get == txn
    checkVal shouldEqual true
  }

  override def afterAll(): Unit = {
    producer.stop()
    onAfterAll()
  }
}
