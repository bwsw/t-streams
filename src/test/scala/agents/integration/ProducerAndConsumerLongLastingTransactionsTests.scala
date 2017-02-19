package agents.integration

import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.env.ConfigurationOptions
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._

// TODO: FAILED

class ProducerAndConsumerLongLastingTransactionsTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  f.setProperty(ConfigurationOptions.Stream.name, "test_stream").
    setProperty(ConfigurationOptions.Stream.partitionsCount, 3).
    setProperty(ConfigurationOptions.Stream.ttlSec, 60 * 10).
    setProperty(ConfigurationOptions.Coordination.connectionTimeoutMs, 7).
    setProperty(ConfigurationOptions.Coordination.sessionTimeoutMs, 7).
    setProperty(ConfigurationOptions.Producer.transportTimeoutMs, 5).
    setProperty(ConfigurationOptions.Producer.Transaction.ttlMs, 6).
    setProperty(ConfigurationOptions.Producer.Transaction.keepAliveMs, 2).
    setProperty(ConfigurationOptions.Consumer.transactionPreload, 10).
    setProperty(ConfigurationOptions.Consumer.dataPreload, 10)

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
    val timeoutForWaiting = 120
    val totalElementsInTransaction = 10
    val dataToSend1: List[String] = (for (part <- 0 until totalElementsInTransaction) yield "data_to_send_pr1_" + randomString).toList.sorted
    val dataToSend2: List[String] = (for (part <- 0 until totalElementsInTransaction) yield "data_to_send_pr2_" + randomString).toList.sorted

    val waitFirstAtSubscriber = new CountDownLatch(1)
    val waitSecondAtSubscriber = new CountDownLatch(1)
    val waitFirstAtProducer = new CountDownLatch(1)

    val producer1Thread = new Thread(() => {
      val transaction = producer1.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
      waitFirstAtProducer.countDown()
      dataToSend1.foreach { x =>
        transaction.send(x.getBytes())
        Thread.sleep(2000)
      }
      transaction.checkpoint()
      waitFirstAtSubscriber.countDown()
    })

    val producer2Thread = new Thread(() => {
      waitFirstAtProducer.await()
      val transaction = producer2.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
      dataToSend2.foreach { x =>
        transaction.send(x.getBytes())
      }
      transaction.checkpoint()
      waitSecondAtSubscriber.countDown()
    })

    producer1Thread.start()
    producer2Thread.start()

    waitFirstAtSubscriber.await()
    val transaction1Opt = consumer.getTransaction(0)
    val data1 = transaction1Opt.get.getAll().map(i => i.toString).sorted
    data1 shouldBe dataToSend1

    waitSecondAtSubscriber.await()
    val transaction2Opt = consumer.getTransaction(0)
    val data2 = transaction2Opt.get.getAll().map(i => i.toString).sorted
    data2 shouldBe dataToSend2

    producer1Thread.join()
    producer2Thread.join()
  }

  override def afterAll(): Unit = {
    producer1.stop()
    producer2.stop()
    onAfterAll()
  }
}