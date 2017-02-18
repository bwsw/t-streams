package agents.integration

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.Offset.Newest
import com.bwsw.tstreams.agents.consumer.subscriber.Callback
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.env.ConfigurationOptions
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{LocalGeneratorCreator, TestUtils}

/**
  * Created by Ivan Kudryavtsev on 26.08.16.
  */
class SubscriberBasicPubSubTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {


  f.setProperty(ConfigurationOptions.Stream.name, "test_stream").
    setProperty(ConfigurationOptions.Stream.partitionsCount, 3).
    setProperty(ConfigurationOptions.Stream.ttlSec, 60 * 10).
    setProperty(ConfigurationOptions.Coordination.connectionTimeoutMs, 7).
    setProperty(ConfigurationOptions.Coordination.sessionTimeoutMs, 7).
    setProperty(ConfigurationOptions.Producer.transportTimeoutMs, 5).
    setProperty(ConfigurationOptions.Producer.Transaction.ttlMs, 3).
    setProperty(ConfigurationOptions.Producer.Transaction.keepAliveMs, 1).
    setProperty(ConfigurationOptions.Consumer.transactionPreload, 10).
    setProperty(ConfigurationOptions.Consumer.dataPreload, 10)

  it should "handle all transactions produced by producer" in {

    val TOTAL = 100
    val latch = new CountDownLatch(1)

    var subsciberTransactionsAmount = 0
    val producer = f.getProducer[String](
      name = "test_producer",
      transactionGenerator = LocalGeneratorCreator.getGen(),
      converter = stringToArrayByteConverter,
      partitions = Set(0, 1, 2))

    val s = f.getSubscriber[String](name = "sv2",
      transactionGenerator = LocalGeneratorCreator.getGen(),
      converter = arrayByteToStringConverter, partitions = Set(0, 1, 2),
      offset = Newest,
      useLastOffset = false,
      callback = new Callback[String] {
        override def onTransaction(consumer: TransactionOperator[String], transaction: ConsumerTransaction[String]): Unit = this.synchronized {
          subsciberTransactionsAmount += 1
          transaction.getAll()
          if(subsciberTransactionsAmount == TOTAL)
            latch.countDown()
        }
      })
    s.start()
    for (it <- 0 until TOTAL) {
      val transaction = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
      transaction.send("test")
      transaction.checkpoint()
    }
    producer.stop()
    latch.await(10, TimeUnit.SECONDS) shouldBe true
    s.stop()
    subsciberTransactionsAmount shouldBe TOTAL
  }

  it should "handle all transactions produced by two different producers" in {

    val TOTAL = 100
    var subscriberTransactionsAmount = 0
    val latch = new CountDownLatch(1)

    val producer1 = f.getProducer[String](
      name = "test_producer",
      transactionGenerator = LocalGeneratorCreator.getGen(),
      converter = stringToArrayByteConverter,
      partitions = Set(0, 1, 2))

    val s = f.getSubscriber[String](name = "sv2",
      transactionGenerator = LocalGeneratorCreator.getGen(),
      converter = arrayByteToStringConverter, partitions = Set(0, 1, 2),
      offset = Newest,
      useLastOffset = false,
      callback = new Callback[String] {
        override def onTransaction(consumer: TransactionOperator[String], transaction: ConsumerTransaction[String]): Unit = this.synchronized {
          subscriberTransactionsAmount += 1
          if(subscriberTransactionsAmount == TOTAL * 2)
            latch.countDown()
        }
      })
    s.start()
    for (it <- 0 until TOTAL) {
      val transaction = producer1.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
      transaction.send("test")
      transaction.checkpoint()
    }
    producer1.stop()
    val producer2 = f.getProducer[String](
      name = "test_producer2",
      transactionGenerator = LocalGeneratorCreator.getGen(),
      converter = stringToArrayByteConverter,
      partitions = Set(0, 1, 2))

    for (it <- 0 until TOTAL) {
      val transaction = producer2.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
      transaction.send("test")
      transaction.checkpoint()
    }
    producer2.stop()
    latch.await(10, TimeUnit.SECONDS) shouldBe true
    s.stop()
    subscriberTransactionsAmount shouldBe TOTAL * 2
  }

  override def afterAll(): Unit = {
    onAfterAll()
  }
}
