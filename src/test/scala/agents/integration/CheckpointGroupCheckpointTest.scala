package agents.integration

import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.group.CheckpointGroup
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.env.ConfigurationOptions
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._


class CheckpointGroupCheckpointTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  f.setProperty(ConfigurationOptions.Stream.name, "test_stream").
    setProperty(ConfigurationOptions.Stream.partitionsCount, 3).
    setProperty(ConfigurationOptions.Stream.ttlSec, 60 * 10).
    setProperty(ConfigurationOptions.Coordination.connectionTimeoutMs, 7000).
    setProperty(ConfigurationOptions.Coordination.sessionTimeoutMs, 7000).
    setProperty(ConfigurationOptions.Producer.transportTimeoutMs, 5000).
    setProperty(ConfigurationOptions.Producer.Transaction.ttlMs, 6000).
    setProperty(ConfigurationOptions.Producer.Transaction.keepAliveMs, 2000).
    setProperty(ConfigurationOptions.Consumer.transactionPreload, 10).
    setProperty(ConfigurationOptions.Consumer.dataPreload, 10)

  val srv = TestStorageServer.get()
  val storageClient = f.getStorageClient()
  storageClient.createStream("test_stream", 2, 24 * 3600, "")

  val producer = f.getProducer(
    name = "test_producer",
    partitions = Set(0, 1, 2))

  val consumer = f.getConsumer(
    name = "test_consumer",
    partitions = Set(0, 1, 2),
    offset = Oldest,
    useLastOffset = true)

  val consumer2 = f.getConsumer(
    name = "test_consumer",
    partitions = Set(0, 1, 2),
    offset = Oldest,
    useLastOffset = true)

  consumer.start

  "Group commit" should "checkpoint all AgentsGroup state" in {
    val group = new CheckpointGroup()
    group.add(producer)
    group.add(consumer)

    val l1 = new CountDownLatch(1)
    val l2 = new CountDownLatch(1)

    val transaction1 = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)

    srv.notifyProducerTransactionCompleted(t =>
      transaction1.getTransactionID() == t.transactionID && t.state == TransactionStates.Checkpointed, l1.countDown())

    logger.info("Transaction 1 is " + transaction1.getTransactionID.toString)
    transaction1.send("info1".getBytes())
    transaction1.checkpoint()

    l1.await()

    //move consumer offsets
    consumer.getTransaction(0).get

    //open transaction without close
    val transaction2 = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened, 1)

    srv.notifyProducerTransactionCompleted(t =>
      transaction2.getTransactionID() == t.transactionID && t.state == TransactionStates.Checkpointed, l2.countDown())

    logger.info("Transaction 2 is " + transaction2.getTransactionID.toString)
    transaction2.send("info2".getBytes())

    group.checkpoint()

    l2.await()

    consumer2.start()
    //assert that the second transaction was closed and consumer offsets was moved
    consumer2.getTransaction(1).get.getAll().head shouldBe "info2".getBytes()
  }

  override def afterAll(): Unit = {
    producer.stop()
    TestStorageServer.dispose(srv)
    onAfterAll()
  }
}
