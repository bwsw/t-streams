package env

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.consumer.subscriber.Callback
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.env.ConfigurationOptions
import com.bwsw.tstreams.generator.LocalTransactionGenerator

/**
  * Created by Ivan Kudryavtsev on 23.07.16.
  */

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.TestUtils

class TStreamsFactoryTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  f.setProperty(ConfigurationOptions.Stream.ttlSec, 60 * 10)


  "If copied" should "contain same data" in {
    val n1 = f.copy()
    n1.setProperty(ConfigurationOptions.Stream.name, "cloned-stream")
    val n2 = n1.copy()
    n2.getProperty(ConfigurationOptions.Stream.name) shouldBe "cloned-stream"
  }

  "If locked" should "raise IllegalStateException exception" in {
    val n1 = f.copy()
    n1.lock()
    val res = try {
      n1.setProperty(ConfigurationOptions.Stream.name, "cloned-stream")
      false
    } catch {
      case e: IllegalStateException =>
        true
    }
    res shouldBe true
  }

  "UniversalFactory.getProducer" should "return producer object" in {
    val p = f.getProducer(
      name = "test-producer-1",
      transactionGenerator = new LocalTransactionGenerator,
      partitions = Set(0))

    p != null shouldEqual true

    p.stop()

  }

  "UniversalFactory.getConsumer" should "return consumer object" in {
    val c = f.getConsumer(
      name = "test-consumer-1",
      transactionGenerator = new LocalTransactionGenerator,
      partitions = Set(0),
      offset = Oldest)

    c != null shouldEqual true

  }

  "UniversalFactory.getSubscriber" should "return subscriber object" in {
    val sub = f.getSubscriber(
      name = "test-subscriber",
      transactionGenerator = new LocalTransactionGenerator,
      partitions = Set(0),
      offset = Oldest,
      callback = new Callback {
        override def onTransaction(consumer: TransactionOperator, transaction: ConsumerTransaction): Unit = {}
      })

    sub != null shouldEqual true
    sub.start()
    Thread.sleep(1000) // TODO: fix it. Bug #31
    sub.stop()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

}
