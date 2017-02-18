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
  * Created by Ivan Kudryavtsev on 14.09.16.
  */
class ProducerWritesToOneSubscriberReadsFromAllTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
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

  val TOTAL = 1000
  val l = new CountDownLatch(1)

  it should "handle all transactions produced by producer" in {
    var subscriberTransactionsAmount = 0
    val producer = f.getProducer(
      name = "test_producer",
      transactionGenerator = LocalGeneratorCreator.getGen(),
      partitions = Set(0))

    val s = f.getSubscriber(name = "sv2",
      transactionGenerator = LocalGeneratorCreator.getGen(),
      partitions = Set(0, 1, 2),
      offset = Newest,
      useLastOffset = true,
      callback = new Callback {
        override def onTransaction(consumer: TransactionOperator, transaction: ConsumerTransaction): Unit = this.synchronized {
          subscriberTransactionsAmount += 1
          if(subscriberTransactionsAmount == TOTAL)
            l.countDown()
        }
      })
    s.start()
    for (it <- 0 until TOTAL) {
      val transaction = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
      transaction.send("test")
      transaction.checkpoint()
    }
    producer.stop()
    l.await(1000, TimeUnit.MILLISECONDS)
    s.stop()
    subscriberTransactionsAmount shouldBe TOTAL
  }

  override def afterAll(): Unit = {
    onAfterAll()
  }
}

