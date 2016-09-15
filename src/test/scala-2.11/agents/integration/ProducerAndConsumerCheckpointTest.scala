package agents.integration

import com.bwsw.tstreams.agents.consumer.ConsumerTransaction
import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.common.TimeTracker
import com.bwsw.tstreams.env.TSF_Dictionary
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._


class ProducerAndConsumerCheckpointTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  f.setProperty(TSF_Dictionary.Stream.NAME, "test_stream").
    setProperty(TSF_Dictionary.Stream.PARTITIONS, 3).
    setProperty(TSF_Dictionary.Stream.TTL, 60 * 10).
    setProperty(TSF_Dictionary.Coordination.CONNECTION_TIMEOUT, 7).
    setProperty(TSF_Dictionary.Coordination.TTL, 7).
    setProperty(TSF_Dictionary.Producer.TRANSPORT_TIMEOUT, 5).
    setProperty(TSF_Dictionary.Producer.Transaction.TTL, 6).
    setProperty(TSF_Dictionary.Producer.Transaction.KEEP_ALIVE, 2).
    setProperty(TSF_Dictionary.Consumer.TRANSACTION_PRELOAD, 10).
    setProperty(TSF_Dictionary.Consumer.DATA_PRELOAD, 10).
    setProperty(TSF_Dictionary.Producer.Transaction.DATA_WRITE_BATCH_SIZE, 100)

  val producer = f.getProducer[String](
    name = "test_producer",
    transactionGenerator = LocalGeneratorCreator.getGen(),
    converter = stringToArrayByteConverter,
    partitions = Set(0),
    isLowPriority = false)

  val consumer = f.getConsumer[String](
    name = "test_consumer",
    transactionGenerator = LocalGeneratorCreator.getGen(),
    converter = arrayByteToStringConverter,
    partitions = Set(0),
    offset = Oldest,
    isUseLastOffset = true)

  val consumer2 = f.getConsumer[String](
    name = "test_consumer",
    transactionGenerator = LocalGeneratorCreator.getGen(),
    converter = arrayByteToStringConverter,
    partitions = Set(0),
    offset = Oldest,
    isUseLastOffset = true)


  "producer, consumer" should "producer - generate many transactions, consumer - retrieve all of them with reinitialization after some time" in {
    val dataToSend = (for (i <- 0 until 100) yield randomString).sorted
    val transactionsAmount = 1000

    (0 until transactionsAmount) foreach { _ =>
      TimeTracker.update_start("newTransaction")
      val transaction = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
      TimeTracker.update_end("newTransaction")
      dataToSend foreach { part =>
        transaction.send(part)
      }
      TimeTracker.update_start("checkpoint")
      transaction.checkpoint()
      TimeTracker.update_end("checkpoint")
    }
    val firstPart = transactionsAmount / 3
    val secondPart = transactionsAmount - firstPart

    var checkVal = true

    consumer.start
    (0 until firstPart) foreach { _ =>
      val transaction: ConsumerTransaction[String] = consumer.getTransaction(0).get
      val data = transaction.getAll().sorted
      consumer.checkpoint()
      checkVal &= data == dataToSend
    }

    consumer2.start
    (0 until secondPart) foreach { _ =>
      val transaction: ConsumerTransaction[String] = consumer2.getTransaction(0).get
      val data = transaction.getAll().sorted
      checkVal &= data == dataToSend
    }

    //assert that is nothing to read
    (0 until Integer.parseInt(f.getProperty(TSF_Dictionary.Stream.PARTITIONS).toString)) foreach { _ =>
      checkVal &= consumer2.getTransaction(0).isEmpty
    }

    checkVal shouldBe true
  }

  override def afterAll(): Unit = {
    producer.stop()
    onAfterAll()
    TimeTracker.dump()
  }
}