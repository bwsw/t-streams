package agents.consumer

import java.net.InetSocketAddress
import java.util.UUID

import com.bwsw.tstreams.agents.consumer.Offsets.Oldest
import com.bwsw.tstreams.agents.consumer.{BasicConsumer, BasicConsumerOptions, BasicConsumerTransaction}
import com.bwsw.tstreams.agents.producer.InsertionType.SingleElementInsert
import com.bwsw.tstreams.agents.producer.{BasicProducer, BasicProducerOptions, ProducerCoordinationOptions, ProducerPolicies}
import com.bwsw.tstreams.coordination.transactions.transport.impl.TcpTransport
import com.bwsw.tstreams.entities.CommitEntity
import com.bwsw.tstreams.streams.BasicStream
import com.datastax.driver.core.utils.UUIDs
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._


class BasicConsumerTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  val aerospikeInstForProducer = storageFactory.getInstance(aerospikeOptions)
  val aerospikeInstForConsumer = storageFactory.getInstance(aerospikeOptions)

  val metadataStorageInstForProducer = metadataStorageFactory.getInstance(
    cassandraHosts = List(new InetSocketAddress("localhost", 9042)),
    keyspace = randomKeyspace)
  val metadataStorageInstForConsumer = metadataStorageFactory.getInstance(
    cassandraHosts = List(new InetSocketAddress("localhost", 9042)),
    keyspace = randomKeyspace)

  val streamForProducer: BasicStream[Array[Byte]] = new BasicStream[Array[Byte]](
    name = "test_stream",
    partitions = 3,
    metadataStorage = metadataStorageInstForProducer,
    dataStorage = aerospikeInstForProducer,
    ttl = 60 * 10,
    description = "some_description")

  val streamForConsumer = new BasicStream[Array[Byte]](
    name = "test_stream",
    partitions = 3,
    metadataStorage = metadataStorageInstForConsumer,
    dataStorage = aerospikeInstForConsumer,
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

  val producerOptions = new BasicProducerOptions[String](transactionTTL = 6, transactionKeepAliveInterval = 2, RoundRobinPolicyCreator.getRoundRobinPolicy(streamForProducer, List(0, 1, 2)), SingleElementInsert, LocalGeneratorCreator.getGen(), agentSettings, stringToArrayByteConverter)

  val consumerOptions = new BasicConsumerOptions[Array[Byte], String](
    transactionsPreload = 10,
    dataPreload = 7,
    consumerKeepAliveInterval = 5,
    arrayByteToStringConverter,
    RoundRobinPolicyCreator.getRoundRobinPolicy(streamForConsumer, List(0, 1, 2)),
    Oldest,
    LocalGeneratorCreator.getGen(),
    useLastOffset = true)

  val producer = new BasicProducer("test_producer", streamForProducer, producerOptions)
  val consumer = new BasicConsumer("test_consumer", streamForConsumer, consumerOptions)
  val connectedSession = cluster.connect(randomKeyspace)

  "consumer.getTransaction" should "return None if nothing was sent" in {
    val txn = consumer.getTransaction
    txn.isEmpty shouldBe true
  }

  "consumer.getTransactionById" should "return sent transaction" in {
    val totalDataInTxn = 10
    val data = (for (i <- 0 until totalDataInTxn) yield randomString).toList.sorted
    val txn = producer.newTransaction(ProducerPolicies.errorIfOpened, 1)
    val txnUuid = txn.getTxnUUID
    data.foreach(x => txn.send(x))
    txn.checkpoint()
    Thread.sleep(2000)
    var checkVal = true

    val consumedTxn = consumer.getTransactionById(1, txnUuid).get
    checkVal = consumedTxn.getPartition == txn.getPartition
    checkVal = consumedTxn.getTxnUUID == txnUuid
    checkVal = consumedTxn.getAll().sorted == data

    checkVal shouldEqual true
  }

  "consumer.getTransaction" should "return sent transaction" in {
    val txn = consumer.getTransaction
    txn.isDefined shouldEqual true
  }

  "consumer.getLastTransaction" should "return last closed transaction" in {
    val commitEntity = new CommitEntity("commit_log", connectedSession)
    val txns = for (i <- 0 until 500) yield UUIDs.timeBased()

    val txn: UUID = txns.head

    commitEntity.commit("test_stream", 1, txns.head, 1, 120)

    txns.drop(1) foreach { x =>
      commitEntity.commit("test_stream", 1, x, -1, 120)
    }

    val retrievedTxnOpt: Option[BasicConsumerTransaction[Array[Byte], String]] = consumer.getLastTransaction(partition = 1)
    val retrievedTxn = retrievedTxnOpt.get
    retrievedTxn.getTxnUUID shouldEqual txn
  }

  override def afterAll(): Unit = {
    producer.stop()
    onAfterAll()
  }
}