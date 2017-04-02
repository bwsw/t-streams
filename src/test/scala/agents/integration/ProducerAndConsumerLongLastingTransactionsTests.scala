package agents.integration

import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.env.ConfigurationOptions
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._

// TODO: FAILED

class ProducerAndConsumerLongLastingTransactionsTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

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
  storageClient.createStream("test_stream", 3, 24 * 3600, "")

  val producer1 = f.getProducer(
    name = "test_producer",
    partitions = Set(0))

  val producer2 = f.getProducer(
    name = "test_producer",
    partitions = Set(0))

  val consumer = f.getConsumer(
    name = "test_consumer",
    partitions = Set(0),
    offset = Oldest,
    useLastOffset = true)

  consumer.start()

  "two producers, consumer" should "first producer - generate transactions lazily, second producer - generate transactions faster" +
    " than the first one but with pause at the very beginning, consumer - retrieve all transactions which was sent" in {
    val totalElementsInTransaction = 10
    val dataToSend1: List[String] = (for (part <- 0 until totalElementsInTransaction) yield "data_to_send_pr1_" + randomKeyspace).toList.sorted
    val dataToSend2: List[String] = (for (part <- 0 until totalElementsInTransaction) yield "data_to_send_pr2_" + randomKeyspace).toList.sorted

    val waitFirstAtConsumer = new CountDownLatch(1)
    val waitSecondAtConsumer = new CountDownLatch(1)
    val waitFirstAtProducer = new CountDownLatch(1)
    val waitSecondAtProducer = new CountDownLatch(1)

    val transaction1 = producer1.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    val transaction2 = producer2.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    srv.notifyProducerTransactionCompleted(t => t.transactionID == transaction1.getTransactionID() && t.state == TransactionStates.Checkpointed, waitFirstAtConsumer.countDown())
    srv.notifyProducerTransactionCompleted(t => t.transactionID == transaction2.getTransactionID() && t.state == TransactionStates.Checkpointed, waitSecondAtConsumer.countDown())

    val producer1Thread = new Thread(() => {
      waitFirstAtProducer.countDown()
      dataToSend1.foreach { x => transaction1.send(x.getBytes()) }
      waitSecondAtProducer.await()
      transaction1.checkpoint()
    })

    val producer2Thread = new Thread(() => {
      waitFirstAtProducer.await()
      dataToSend2.foreach { x => transaction2.send(x.getBytes()) }
      transaction2.checkpoint()
      waitSecondAtProducer.countDown()
    })

    Seq(producer1Thread, producer2Thread).foreach(t => t.start())

    waitFirstAtConsumer.await()
    val transaction1Opt = consumer.getTransaction(0)
    transaction1Opt.get.getTransactionID() shouldBe transaction1.getTransactionID()
    val data1 = transaction1Opt.get.getAll().map(i => new String(i)).toList.sorted
    data1 shouldBe dataToSend1

    waitSecondAtConsumer.await()
    val transaction2Opt = consumer.getTransaction(0)
    transaction2Opt.get.getTransactionID() shouldBe transaction2.getTransactionID()
    val data2 = transaction2Opt.get.getAll().map(i => new String(i)).toList.sorted
    data2 shouldBe dataToSend2

    producer1Thread.join()
    producer2Thread.join()
  }

  override def afterAll(): Unit = {
    producer1.stop()
    producer2.stop()
    TestStorageServer.dispose(srv)
    onAfterAll()
  }
}