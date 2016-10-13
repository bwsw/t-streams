package agents.integration

import com.bwsw.tstreams.agents.consumer.ConsumerTransaction
import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.common.CassandraHelper
import com.bwsw.tstreams.env.TSF_Dictionary
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._


class ProducerAndConsumerSimpleTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  f.setProperty(TSF_Dictionary.Stream.NAME, "test_stream").
    setProperty(TSF_Dictionary.Stream.PARTITIONS, 3).
    setProperty(TSF_Dictionary.Stream.TTL, 60 * 10).
    setProperty(TSF_Dictionary.Coordination.CONNECTION_TIMEOUT, 7).
    setProperty(TSF_Dictionary.Coordination.TTL, 7).
    setProperty(TSF_Dictionary.Producer.TRANSPORT_TIMEOUT, 5).
    setProperty(TSF_Dictionary.Producer.Transaction.TTL, 6).
    setProperty(TSF_Dictionary.Producer.Transaction.KEEP_ALIVE, 2).
    setProperty(TSF_Dictionary.Consumer.TRANSACTION_PRELOAD, 10).
    setProperty(TSF_Dictionary.Consumer.DATA_PRELOAD, 10)

  val producer = f.getProducer[String](
    name = "test_producer",
    transactionGenerator = LocalGeneratorCreator.getGen(),
    converter = stringToArrayByteConverter,
    partitions = Set(0))

  val consumer = f.getConsumer[String](
    name = "test_consumer",
    transactionGenerator = LocalGeneratorCreator.getGen(),
    converter = arrayByteToStringConverter,
    partitions = Set(0),
    offset = Oldest,
    isUseLastOffset = true)

  consumer.start()


  "producer, consumer" should "producer - generate one transaction, consumer - retrieve it with getAll method" in {
    CassandraHelper.clearMetadataTables(session, randomKeyspace)
    val totalDataInTransaction = 10
    val producerTransaction = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    val sendData = (for (part <- 0 until totalDataInTransaction) yield "data_part_" + randomString).sorted
    sendData.foreach { x =>
      producerTransaction.send(x)
    }
    producerTransaction.checkpoint()
    Thread.sleep(100)
    val transaction = consumer.getTransaction(0).get

    var checkVal = transaction.getAll().sorted == sendData

    //assert that is nothing to read
    (0 until consumer.stream.getPartitions) foreach { _ =>
      checkVal &= consumer.getTransaction(0).isEmpty
    }

    checkVal shouldEqual true
  }

  "producer, consumer" should "producer - generate one transaction, consumer - retrieve it using iterator" in {
    CassandraHelper.clearMetadataTables(session, randomKeyspace)
    val totalDataInTransaction = 10
    val producerTransaction = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    val sendData = (for (part <- 0 until totalDataInTransaction) yield "data_part_" + randomString).sorted
    sendData.foreach { x =>
      producerTransaction.send(x)
    }
    producerTransaction.checkpoint()
    val transactionOpt = consumer.getTransaction(0)
    assert(transactionOpt.isDefined)
    val transaction = transactionOpt.get
    var dataToAssert = ListBuffer[String]()
    while (transaction.hasNext()) {
      dataToAssert += transaction.next()
    }

    var checkVal = true

    //assert that is nothing to read
    (0 until consumer.stream.getPartitions) foreach { _ =>
      checkVal &= consumer.getTransaction(0).isEmpty
    }

    checkVal &= dataToAssert.toList.sorted == sendData

    checkVal shouldEqual true
  }

  "producer, consumer" should "producer - generate some set of transactions, consumer - retrieve them all" in {
    CassandraHelper.clearMetadataTables(session, randomKeyspace)
    val transactionsAmount = 100
    val totalDataInTransaction = 10
    val sendData = (for (part <- 0 until totalDataInTransaction) yield "data_part_" + randomString).sorted

    (0 until transactionsAmount).foreach { _ =>
      val producerTransaction = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
      sendData.foreach { x =>
        producerTransaction.send(x)
      }
      producerTransaction.checkpoint()
    }

    var checkVal = true

    (0 until transactionsAmount).foreach { _ =>
      val transaction = consumer.getTransaction(0)
      checkVal &= transaction.nonEmpty
      checkVal &= transaction.get.getAll().sorted == sendData
    }

    //assert that is nothing to read
    (0 until consumer.stream.getPartitions) foreach { _ =>
      checkVal &= consumer.getTransaction(0).isEmpty
    }

    checkVal shouldBe true
  }

  "producer, consumer" should "producer - generate transaction, consumer retrieve it (both start async)" in {
    CassandraHelper.clearMetadataTables(session, randomKeyspace)
    val timeoutForWaiting = 120
    val totalDataInTransaction = 10
    val sendData = (for (part <- 0 until totalDataInTransaction) yield "data_part_" + part).sorted

    val producerThread = new Thread(new Runnable {
      def run() {
        val transaction = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
        sendData.foreach { x =>
          transaction.send(x)
          Thread.sleep(1000)
        }
        transaction.checkpoint()
      }
    })

    var checkVal = true

    val consumerThread = new Thread(new Runnable {
      def run() {
        breakable {
          while (true) {
            val consumedTransaction: Option[ConsumerTransaction[String]] = consumer.getTransaction(0)
            if (consumedTransaction.isDefined) {
              checkVal &= consumedTransaction.get.getAll().sorted == sendData
              break()
            }
            Thread.sleep(1000)
          }
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
    (0 until consumer.stream.getPartitions) foreach { _ =>
      checkVal &= consumer.getTransaction(0).isEmpty
    }

    checkVal shouldEqual true
  }

  override def afterAll(): Unit = {
    producer.stop()
    onAfterAll()
  }
}

