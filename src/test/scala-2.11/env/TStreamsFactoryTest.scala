package env

import java.util.UUID

import com.bwsw.tstreams.agents.consumer.Offsets.Oldest
import com.bwsw.tstreams.agents.consumer.subscriber.{BasicSubscribingConsumer, BasicSubscriberCallback}
import com.bwsw.tstreams.agents.producer.ProducerPolicies
import com.bwsw.tstreams.converter.{ArrayByteToStringConverter, StringToArrayByteConverter}
import com.bwsw.tstreams.env.{TSF_Dictionary, TStreamsFactory}

/**
  * Created by ivan on 23.07.16.
  */
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._


class TStreamsFactoryTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  val randomKeyspace = createRandomKeyspace

  val f = new TStreamsFactory()
  f.setProperty(TSF_Dictionary.Metadata.Cluster.namespace,randomKeyspace).
    setProperty(TSF_Dictionary.Data.Cluster.namespace,"test").
    setProperty(TSF_Dictionary.Stream.name, "test-stream")

  "UniversalFactory.getProducer" should "return producer object" in {
    val p = f.getProducer[String](
      name          = "test-producer-1",
      isLowPriority = false,
      txnGenerator  = LocalGeneratorCreator.getGen(),
      converter     = new StringToArrayByteConverter,
      partitions    = List(0) )

    p != null shouldEqual true

    p.stop()

  }

  "UniversalFactory.getConsumer" should "return consumer object" in {
    val c = f.getConsumer[String](
      name          = "test-consumer-1",
      txnGenerator  = LocalGeneratorCreator.getGen(),
      converter     = new ArrayByteToStringConverter,
      partitions    = List(0),
      offset        = Oldest)

    c != null shouldEqual true

  }

  "UniversalFactory.getSubscriber" should "return subscriber object" in {
    val sub = f.getSubscriber[String](
      name = "test-subscriber",
      txnGenerator = LocalGeneratorCreator.getGen,
      converter = new ArrayByteToStringConverter,
      partitions = List(0),
      offset = Oldest,
      callback = new BasicSubscriberCallback[Array[Byte],String] {
          override def onEvent(subscriber: BasicSubscribingConsumer[Array[Byte], String], partition: Int, transactionUuid: UUID): Unit = {}
          override val pollingFrequency: Int = 1
      })

    sub != null shouldEqual true
    Thread.sleep(2000) // TODO: fix it. Bug #31
    sub.stop()
  }

  override def afterAll(): Unit = {
    f.close()
    super.afterAll()
  }

}
