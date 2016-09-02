package agents.integration

import java.util.UUID
import java.util.concurrent.{TimeUnit, CountDownLatch}

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.consumer.TransactionOperator
import com.bwsw.tstreams.agents.consumer.subscriber.Callback
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.env.TSF_Dictionary
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import testutils.{LocalGeneratorCreator, TestUtils}

/**
  * Created by Ivan Kudryavtsev on 24.08.16.
  */
class SubscriberBasicFunctions extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils  {
  f.setProperty(TSF_Dictionary.Stream.NAME,"test_stream").
    setProperty(TSF_Dictionary.Stream.PARTITIONS,3).
    setProperty(TSF_Dictionary.Stream.TTL, 60 * 10).
    setProperty(TSF_Dictionary.Coordination.CONNECTION_TIMEOUT, 7).
    setProperty(TSF_Dictionary.Coordination.TTL, 7).
    setProperty(TSF_Dictionary.Producer.TRANSPORT_TIMEOUT, 5).
    setProperty(TSF_Dictionary.Producer.Transaction.TTL, 3).
    setProperty(TSF_Dictionary.Producer.Transaction.KEEP_ALIVE, 1).
    setProperty(TSF_Dictionary.Consumer.TRANSACTION_PRELOAD, 10).
    setProperty(TSF_Dictionary.Consumer.DATA_PRELOAD, 10)

  val producer = f.getProducer[String](
    name = "test_producer",
    txnGenerator = LocalGeneratorCreator.getGen(),
    converter = stringToArrayByteConverter,
    partitions = Set(0,1,2),
    isLowPriority = false)

  it should "start and stop with default options" in {
    val s = f.getSubscriber[String](name = "sv2",
      txnGenerator = LocalGeneratorCreator.getGen(),
      converter = arrayByteToStringConverter, partitions = Set(0,1,2),
      offset = Oldest,
      isUseLastOffset = true,
      callback = new Callback[String] {
        override def onEvent(consumer: TransactionOperator[String], partition: Int, uuid: UUID, count: Int): Unit = {}
      })
    s.start()
    s.stop()
  }

  it should "allow start and stop several times" in {
    val s = f.getSubscriber[String](name = "sv2",
      txnGenerator = LocalGeneratorCreator.getGen(),
      converter = arrayByteToStringConverter, partitions = Set(0,1,2),
      offset = Oldest,
      isUseLastOffset = true,
      callback = new Callback[String] {
        override def onEvent(consumer: TransactionOperator[String], partition: Int, uuid: UUID, count: Int): Unit = {}
      })
    s.start()
    s.stop()
    s.start()
    s.stop()
  }

  it should "not allow double start" in {
    val s = f.getSubscriber[String](name = "sv2",
      txnGenerator = LocalGeneratorCreator.getGen(),
      converter = arrayByteToStringConverter, partitions = Set(0,1,2),
      offset = Oldest,
      isUseLastOffset = true,
      callback = new Callback[String] {
        override def onEvent(consumer: TransactionOperator[String], partition: Int, uuid: UUID, count: Int): Unit = {}
      })
    s.start()
    var flag = false
    flag = try {
      s.start()
      false
    } catch {
      case e: IllegalStateException =>
        true
    }
    flag shouldBe true
    s.stop()
  }

  it should "not allow double stop" in {
    val s = f.getSubscriber[String](name = "sv2",
      txnGenerator = LocalGeneratorCreator.getGen(),
      converter = arrayByteToStringConverter, partitions = Set(0,1,2),
      offset = Oldest,
      isUseLastOffset = true,
      callback = new Callback[String] {
        override def onEvent(consumer: TransactionOperator[String], partition: Int, uuid: UUID, count: Int): Unit = {}
      })
    s.start()
    s.stop()
    var flag = false
    flag = try {
      s.stop()
      false
    } catch {
      case e: IllegalStateException =>
        true
    }
    flag shouldBe true
  }

  it should "allow to be created with in memory queues" in {
    val f1 = f.copy()
    f1.setProperty(TSF_Dictionary.Consumer.Subscriber.PERSISTENT_QUEUE_PATH, null)
    val s = f1.getSubscriber[String](name = "sv2_inram",
      txnGenerator = LocalGeneratorCreator.getGen(),
      converter = arrayByteToStringConverter, partitions = Set(0,1,2),
      offset = Oldest,
      isUseLastOffset = true,
      callback = new Callback[String] {
        override def onEvent(consumer: TransactionOperator[String], partition: Int, uuid: UUID, count: Int): Unit = {}
      })
    s.start()
    s.stop()
  }

    it should "receive all transactions producer by producer previously" in {
    val l = new CountDownLatch(1)
    var i: Int = 0
    val TOTAL = 1000
    var uuid: UUID = null
    for(it <- 0 until TOTAL) {
      val txn = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
      txn.send("test")
      txn.checkpoint()
      uuid = txn.getTransactionUUID()
    }
    producer.stop()

    val s = f.getSubscriber[String](name = "sv2",
      txnGenerator = LocalGeneratorCreator.getGen(),
      converter = arrayByteToStringConverter, partitions = Set(0,1,2),
      offset = Oldest,
      isUseLastOffset = true,
      callback = new Callback[String] {
        override def onEvent(consumer: TransactionOperator[String], partition: Int, uuid: UUID, count: Int): Unit = this.synchronized {
          i += 1
          if(i == TOTAL)
            l.countDown()
        }
      })
    s.start()
    l.await(10, TimeUnit.SECONDS)
    s.stop()
    i shouldBe TOTAL
  }

  override def afterAll(): Unit = {
    onAfterAll()
  }
}
