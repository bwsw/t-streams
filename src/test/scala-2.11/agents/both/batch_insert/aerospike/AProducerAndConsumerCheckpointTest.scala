package agents.both.batch_insert.aerospike

import com.bwsw.tstreams.agents.consumer.Offsets.Oldest
import com.bwsw.tstreams.agents.consumer.{Consumer, ConsumerTransaction}
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.common.TimeTracker
import com.bwsw.tstreams.env.TSF_Dictionary
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._


class AProducerAndConsumerCheckpointTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  f.setProperty(TSF_Dictionary.Stream.NAME,"test_stream").
    setProperty(TSF_Dictionary.Stream.PARTITIONS,3).
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
    txnGenerator = LocalGeneratorCreator.getGen(),
    converter = stringToArrayByteConverter,
    partitions = List(0,1,2),
    isLowPriority = false)

  val consumer = f.getConsumer[String](
    name = "test_consumer",
    txnGenerator = LocalGeneratorCreator.getGen(),
    converter = arrayByteToStringConverter,
    partitions = List(0,1,2),
    offset = Oldest,
    isUseLastOffset = true)

  val consumer2 = f.getConsumer[String](
    name = "test_consumer",
    txnGenerator = LocalGeneratorCreator.getGen(),
    converter = arrayByteToStringConverter,
    partitions = List(0,1,2),
    offset = Oldest,
    isUseLastOffset = true)


  "producer, consumer" should "producer - generate many transactions, consumer - retrieve all of them with reinitialization after some time" in {
    val dataToSend = (for (i <- 0 until 10) yield randomString).sorted
    val txnNum = 1000

    (0 until txnNum) foreach { _ =>
      TimeTracker.update_start("newTxn")
      val txn = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
      TimeTracker.update_end("newTxn")
      dataToSend foreach { part =>
        txn.send(part)
      }
      TimeTracker.update_start("checkpoint")
      txn.checkpoint()
      TimeTracker.update_end("checkpoint")
    }
    val firstPart = txnNum / 3
    val secondPart = txnNum - firstPart

    var checkVal = true

    consumer.start
    (0 until firstPart) foreach { _ =>
      val txn: ConsumerTransaction[String] = consumer.getTransaction.get
      val data = txn.getAll().sorted
      consumer.checkpoint()
      checkVal &= data == dataToSend
    }

    consumer2.start
    (0 until secondPart) foreach { _ =>
      val txn: ConsumerTransaction[String] = consumer2.getTransaction.get
      val data = txn.getAll().sorted
      checkVal &= data == dataToSend
    }

    //assert that is nothing to read
    (0 until Integer.parseInt(f.getProperty(TSF_Dictionary.Stream.PARTITIONS).toString)) foreach { _ =>
      checkVal &= consumer2.getTransaction.isEmpty
    }

    checkVal shouldBe true
  }

  override def afterAll(): Unit = {
    producer.stop()
    onAfterAll()
    TimeTracker.dump()
  }
}