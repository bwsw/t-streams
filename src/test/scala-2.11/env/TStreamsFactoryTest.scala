package env

import java.util.UUID

import com.bwsw.tstreams.agents.consumer.Offsets.Oldest
import com.bwsw.tstreams.agents.consumer.subscriber.{Callback, SubscribingConsumer}
import com.bwsw.tstreams.converter.{ArrayByteToStringConverter, StringToArrayByteConverter}
import com.bwsw.tstreams.env.TSF_Dictionary
import com.bwsw.tstreams.velocity.LocalGeneratorCreator

/**
  * Created by ivan on 23.07.16.
  */

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.TestUtils

class TStreamsFactoryTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  "If copied" should "contain same data" in {
    val n1 = f.copy()
    n1.setProperty(TSF_Dictionary.Stream.NAME, "cloned-stream")
    val n2 = n1.copy()
    n2.getProperty(TSF_Dictionary.Stream.NAME) shouldBe "cloned-stream"
  }

  "If locked" should "raise IllegalStateException exception" in {
    val n1 = f.copy()
    n1.lock()
    try {
      n1.setProperty(TSF_Dictionary.Stream.NAME, "cloned-stream")
      false shouldBe true
    } catch {
      case e: IllegalStateException =>
      true shouldBe true
    }
  }

  "UniversalFactory.getProducer" should "return producer object" in {
    val p = f.getProducer[String](
      name = "test-producer-1",
      isLowPriority = false,
      txnGenerator = LocalGeneratorCreator.getGen(),
      converter = new StringToArrayByteConverter,
      partitions = List(0))

    p != null shouldEqual true

    p.stop()

  }

  "UniversalFactory.getConsumer" should "return consumer object" in {
    val c = f.getConsumer[String](
      name = "test-consumer-1",
      txnGenerator = LocalGeneratorCreator.getGen(),
      converter = new ArrayByteToStringConverter,
      partitions = List(0),
      offset = Oldest)

    c != null shouldEqual true

  }

  "UniversalFactory.getSubscriber" should "return subscriber object" in {
    val sub = f.getSubscriber[String](
      name = "test-subscriber",
      txnGenerator = LocalGeneratorCreator.getGen,
      converter = new ArrayByteToStringConverter,
      partitions = List(0),
      offset = Oldest,
      callback = new Callback[String] {
        override def onEvent(subscriber: SubscribingConsumer[String], partition: Int, transactionUuid: UUID): Unit = {}
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
