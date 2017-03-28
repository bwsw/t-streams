package agents.integration

import com.bwsw.tstreams.agents.consumer.ConsumerTransaction
import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.env.ConfigurationOptions
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._


class ProducerAndConsumerSimpleTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

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
    val totalDataInTransaction = 10
    val producerTransaction = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    val sendData = (for (part <- 0 until totalDataInTransaction) yield "data_part_" + randomKeyspace).sorted
    sendData.foreach { x =>
      producerTransaction.send(x.getBytes())
    }
    producerTransaction.checkpoint()
    Thread.sleep(100)
    val transaction = consumer.getTransaction(0).get

    var checkVal = transaction.getAll().map(i => i.toString).sorted == sendData

    //assert that is nothing to read
    (0 until consumer.stream.partitionsCount) foreach { _ =>
      checkVal &= consumer.getTransaction(0).isEmpty
    }

    checkVal shouldEqual true
  }

  "producer, consumer" should "producer - generate one transaction, consumer - retrieve it using iterator" in {
    val totalDataInTransaction = 10
    val producerTransaction = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    val sendData = (for (part <- 0 until totalDataInTransaction) yield "data_part_" + randomKeyspace).sorted
    sendData.foreach { x =>
      producerTransaction.send(x.getBytes())
    }
    producerTransaction.checkpoint()
    val transactionOpt = consumer.getTransaction(0)
    assert(transactionOpt.isDefined)
    val transaction = transactionOpt.get
    var dataToAssert = ListBuffer[Array[Byte]]()
    while (transaction.hasNext()) {
      dataToAssert += transaction.next()
    }

    var checkVal = true

    //assert that is nothing to read
    (0 until consumer.stream.partitionsCount) foreach { _ =>
      checkVal &= consumer.getTransaction(0).isEmpty
    }

    checkVal &= dataToAssert.toList.map(i => i.toString).sorted == sendData

    checkVal shouldEqual true
  }

  "producer, consumer" should "producer - generate some set of transactions, consumer - retrieve them all" in {
    val transactionsAmount = 100
    val totalDataInTransaction = 10
    val sendData = (for (part <- 0 until totalDataInTransaction) yield "data_part_" + randomKeyspace).sorted

    (0 until transactionsAmount).foreach { _ =>
      val producerTransaction = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
      sendData.foreach { x =>
        producerTransaction.send(x.getBytes())
      }
      producerTransaction.checkpoint()
    }

    var checkVal = true

    (0 until transactionsAmount).foreach { _ =>
      val transaction = consumer.getTransaction(0)
      checkVal &= transaction.nonEmpty
      checkVal &= transaction.get.getAll().map(i => i.toString).sorted == sendData
    }

    //assert that is nothing to read
    (0 until consumer.stream.partitionsCount) foreach { _ =>
      checkVal &= consumer.getTransaction(0).isEmpty
    }

    checkVal shouldBe true
  }

  "producer, consumer" should "producer - generate transaction, consumer retrieve it (both start async)" in {
    val timeoutForWaiting = 120
    val totalDataInTransaction = 10
    val sendData = (for (part <- 0 until totalDataInTransaction) yield "data_part_" + part).sorted

    val producerThread = new Thread(() => {
      val transaction = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
      sendData.foreach { x =>
        transaction.send(x.getBytes())
        Thread.sleep(1000)
      }
      transaction.checkpoint()
    })

    var checkVal = true

    val consumerThread = new Thread(() => {
      breakable {
        while (true) {
          val consumedTransaction: Option[ConsumerTransaction] = consumer.getTransaction(0)
          if (consumedTransaction.isDefined) {
            checkVal &= consumedTransaction.get.getAll().map(i => i.toString).sorted == sendData
            break()
          }
          Thread.sleep(1000)
        }
      }
    })

    producerThread.start()
    consumerThread.start()
    producerThread.join(timeoutForWaiting * 1000)
    consumerThread.join(timeoutForWaiting * 1000)

    checkVal &= !producerThread.isAlive
    checkVal &= !consumerThread.isAlive

    //assert that is nothing to read
    (0 until consumer.stream.partitionsCount) foreach { _ =>
      checkVal &= consumer.getTransaction(0).isEmpty
    }

    checkVal shouldEqual true
  }

  override def afterAll(): Unit = {
    producer.stop()
    onAfterAll()
  }
}

