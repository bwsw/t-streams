package agents.integration

import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.agents.consumer.ConsumerTransaction
import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.env.ConfigurationOptions
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._


class ProducerAndConsumerSimpleTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

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

  val producer = f.getProducer(
    name = "test_producer",
    partitions = Set(0))

  val consumer = f.getConsumer(
    name = "test_consumer",
    partitions = Set(0),
    offset = Oldest,
    useLastOffset = true)

  consumer.start()


  "producer, consumer" should "producer - generate one transaction, consumer - retrieve it with getAll method" in {
    val DATA_IN_TRANSACTION = 10

    val producerTransaction = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    val l = new CountDownLatch(1)
    srv.notifyProducerTransactionCompleted(t => t.transactionID == producerTransaction.getTransactionID() && t.state == TransactionStates.Checkpointed, l.countDown())
    val sendData = (for (part <- 0 until DATA_IN_TRANSACTION) yield "data_part_" + randomKeyspace).sorted
    sendData.foreach { x =>
      producerTransaction.send(x.getBytes())
    }
    producerTransaction.checkpoint()
    l.await()

    val transaction = consumer.getTransaction(0).get

    transaction.getAll().map(i => new String(i)).sorted shouldBe sendData

    //assert that is nothing to read
    (0 until consumer.stream.partitionsCount) foreach { _ =>
      consumer.getTransaction(0).isEmpty shouldBe true
    }
  }

  "producer, consumer" should "producer - generate one transaction, consumer - retrieve it using iterator" in {
    val DATA_IN_TRANSACTION = 10
    val sendData = (for (part <- 0 until DATA_IN_TRANSACTION) yield "data_part_" + randomKeyspace).sorted

    val producerTransaction = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    val l = new CountDownLatch(1)
    srv.notifyProducerTransactionCompleted(t => t.transactionID == producerTransaction.getTransactionID() && t.state == TransactionStates.Checkpointed, l.countDown())

    sendData.foreach { x => producerTransaction.send(x.getBytes()) }
    producerTransaction.checkpoint()
    l.await()

    val transactionOpt = consumer.getTransaction(0)
    transactionOpt.isDefined shouldBe true

    val transaction = transactionOpt.get

    var readData = ListBuffer[String]()

    while (transaction.hasNext()) {
      val s = new String(transaction.next())
      readData += s
    }

    readData.toList.sorted shouldBe sendData

    //assert that is nothing to read
    (0 until consumer.stream.partitionsCount) foreach { _ => consumer.getTransaction(0).isEmpty shouldBe true }

  }

  "producer, consumer" should "producer - generate some set of transactions, consumer - retrieve them all" in {
    val TRANSACTIONS_COUNT = 100
    val DATA_IN_TRANSACTION = 10

    val sendData = (for (part <- 0 until DATA_IN_TRANSACTION) yield "data_part_" + randomKeyspace).sorted

    val l = new CountDownLatch(1)

    var counter = 0

    (0 until TRANSACTIONS_COUNT).foreach { _ =>
      val producerTransaction = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)

      counter += 1
      if (counter == TRANSACTIONS_COUNT)
        srv.notifyProducerTransactionCompleted(t => t.transactionID == producerTransaction.getTransactionID() && t.state == TransactionStates.Checkpointed, l.countDown())

      sendData.foreach { x => producerTransaction.send(x.getBytes()) }
      producerTransaction.checkpoint()
    }

    l.await()

    (0 until TRANSACTIONS_COUNT).foreach { _ =>
      val transaction = consumer.getTransaction(0)
      transaction.nonEmpty shouldBe true
      transaction.get.getAll().map(i => new String(i)).sorted == sendData
    }

    //assert that is nothing to read
    (0 until consumer.stream.partitionsCount) foreach { _ =>
      consumer.getTransaction(0).isEmpty shouldBe true
    }
  }

  "producer, consumer" should "producer - generate some set of transactions after cancel, consumer - retrieve them all" in {
    val TRANSACTIONS_COUNT = 100
    val DATA_IN_TRANSACTION = 1

    val pl = ListBuffer[Long]()
    val cl = ListBuffer[Long]()

    val sendData = (for (part <- 0 until DATA_IN_TRANSACTION) yield "data_part_" + randomKeyspace).sorted

    val l = new CountDownLatch(1)

    var counter = 0

    val producerTransaction = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    producerTransaction.cancel()

    (0 until TRANSACTIONS_COUNT).foreach { _ =>
      val producerTransaction = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)

      pl.append(producerTransaction.getTransactionID())

      counter += 1
      if (counter == TRANSACTIONS_COUNT)
        srv.notifyProducerTransactionCompleted(t => t.transactionID == producerTransaction.getTransactionID() && t.state == TransactionStates.Checkpointed, l.countDown())

      sendData.foreach { x => producerTransaction.send(x.getBytes()) }
      producerTransaction.checkpoint()
    }

    l.await()
    (0 until TRANSACTIONS_COUNT).foreach { i =>
      val transactionOpt = consumer.getTransaction(0)
      transactionOpt.nonEmpty shouldBe true
      cl.append(transactionOpt.get.getTransactionID())
    }

    cl shouldBe pl

  }

  "producer, consumer" should "producer - generate transaction, consumer retrieve it (both start async)" in {
    val timeoutForWaiting = 5
    val DATA_IN_TRANSACTION = 10

    val sendData = (for (part <- 0 until DATA_IN_TRANSACTION) yield "data_part_" + part).sorted

    val producerThread = new Thread(() => {
      val transaction = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
      Thread.sleep(100)
      sendData.foreach { x =>
        transaction.send(x.getBytes())
      }
      transaction.checkpoint()
    })

    val consumerThread = new Thread(() => {
      breakable {
        while (true) {
          val consumedTransaction: Option[ConsumerTransaction] = consumer.getTransaction(0)
          if (consumedTransaction.isDefined) {
            consumedTransaction.get.getAll().map(i => new String(i)).sorted shouldBe sendData
            break()
          }
          Thread.sleep(100)
        }
      }
    })

    producerThread.start()
    consumerThread.start()
    producerThread.join(timeoutForWaiting * 1000)
    consumerThread.join(timeoutForWaiting * 1000)

    consumerThread.isAlive shouldBe false
    producerThread.isAlive shouldBe false

    (0 until consumer.stream.partitionsCount) foreach { _ => consumer.getTransaction(0).isEmpty shouldBe true }

  }

  override def afterAll(): Unit = {
    producer.stop()
    TestStorageServer.dispose(srv)
    onAfterAll()
  }
}

