package agents.integration

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.consumer.subscriber.Callback
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.env.ConfigurationOptions
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{LocalGeneratorCreator, TestUtils}

/**
  * Created by Ivan Kudryavtsev on 24.08.16.
  */
class SubscriberBasicFunctionsTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  f.setProperty(ConfigurationOptions.Stream.name, "test_stream").
    setProperty(ConfigurationOptions.Stream.partitionsCount, 3).
    setProperty(ConfigurationOptions.Stream.ttlSec, 60 * 10).
    setProperty(ConfigurationOptions.Coordination.connectionTimeoutMs, 7).
    setProperty(ConfigurationOptions.Coordination.sessionTimeoutMs, 7).
    setProperty(ConfigurationOptions.Producer.transportTimeoutMs, 5).
    setProperty(ConfigurationOptions.Producer.Transaction.ttlMs, 3).
    setProperty(ConfigurationOptions.Producer.Transaction.keepAliveMs, 1).
    setProperty(ConfigurationOptions.Consumer.TRANSACTION_PRELOAD, 10).
    setProperty(ConfigurationOptions.Consumer.DATA_PRELOAD, 10)

  val producer = f.getProducer[String](
    name = "test_producer",
    transactionGenerator = LocalGeneratorCreator.getGen(),
    converter = stringToArrayByteConverter,
    partitions = Set(0, 1, 2))

  it should "start and stop with default options" in {
    val s = f.getSubscriber[String](name = "sv2",
      transactionGenerator = LocalGeneratorCreator.getGen(),
      converter = arrayByteToStringConverter, partitions = Set(0, 1, 2),
      offset = Oldest,
      useLastOffset = true,
      callback = new Callback[String] {
        override def onTransaction(consumer: TransactionOperator[String], transaction: ConsumerTransaction[String]): Unit = {}
      })
    s.start()
    s.stop()
  }

  it should "allow start and stop several times" in {
    val s = f.getSubscriber[String](name = "sv2",
      transactionGenerator = LocalGeneratorCreator.getGen(),
      converter = arrayByteToStringConverter, partitions = Set(0, 1, 2),
      offset = Oldest,
      useLastOffset = true,
      callback = new Callback[String] {
        override def onTransaction(consumer: TransactionOperator[String], transaction: ConsumerTransaction[String]): Unit = {}
      })
    s.start()
    s.stop()
    s.start()
    s.stop()
  }

  it should "not allow double start" in {
    val s = f.getSubscriber[String](name = "sv2",
      transactionGenerator = LocalGeneratorCreator.getGen(),
      converter = arrayByteToStringConverter, partitions = Set(0, 1, 2),
      offset = Oldest,
      useLastOffset = true,
      callback = new Callback[String] {
        override def onTransaction(consumer: TransactionOperator[String], transaction: ConsumerTransaction[String]): Unit = {}
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
      transactionGenerator = LocalGeneratorCreator.getGen(),
      converter = arrayByteToStringConverter, partitions = Set(0, 1, 2),
      offset = Oldest,
      useLastOffset = true,
      callback = new Callback[String] {
        override def onTransaction(consumer: TransactionOperator[String], transaction: ConsumerTransaction[String]): Unit = {}
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
    val s = f1.getSubscriber[String](name = "sv2_inram",
      transactionGenerator = LocalGeneratorCreator.getGen(),
      converter = arrayByteToStringConverter, partitions = Set(0, 1, 2),
      offset = Oldest,
      useLastOffset = true,
      callback = new Callback[String] {
        override def onTransaction(consumer: TransactionOperator[String], transaction: ConsumerTransaction[String]): Unit = {}
      })
    s.start()
    s.stop()
  }

  it should "receive all transactions producer by producer previously" in {
    val l = new CountDownLatch(1)
    var i: Int = 0
    val TOTAL = 1000
    var id: Long = 0
    for (it <- 0 until TOTAL) {
      val transaction = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
      transaction.send("test")
      transaction.checkpoint()
      id = transaction.getTransactionID()
    }
    producer.stop()

    val s = f.getSubscriber[String](name = "sv2",
      transactionGenerator = LocalGeneratorCreator.getGen(),
      converter = arrayByteToStringConverter, partitions = Set(0, 1, 2),
      offset = Oldest,
      useLastOffset = true,
      callback = new Callback[String] {
        override def onTransaction(consumer: TransactionOperator[String], transaction: ConsumerTransaction[String]): Unit = this.synchronized {
          i += 1
          if (i == TOTAL)
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
