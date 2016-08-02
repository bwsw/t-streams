package agents.subscriber

import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.agents.consumer.Offsets.Oldest
import com.bwsw.tstreams.agents.consumer.subscriber.{BasicSubscribingConsumer, BasicSubscriberCallback}
import com.bwsw.tstreams.debug.GlobalHooks
import com.bwsw.tstreams.env.TSF_Dictionary
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import testutils.{LocalGeneratorCreator, TestUtils}

/**
  * Created by ivan on 02.08.16.
  */
class AsynchronousTransactionTests  extends FlatSpec with Matchers
  with BeforeAndAfterAll with TestUtils {

  GlobalHooks.addHook("AfterCommitFailure", () => throw new RuntimeException)

  f.setProperty(TSF_Dictionary.Stream.name,"test_stream").
    setProperty(TSF_Dictionary.Stream.partitions,3).
    setProperty(TSF_Dictionary.Stream.ttl, 60 * 10).
    setProperty(TSF_Dictionary.Coordination.connection_timeout, 7).
    setProperty(TSF_Dictionary.Coordination.ttl, 7).
    setProperty(TSF_Dictionary.Producer.master_timeout, 5).
    setProperty(TSF_Dictionary.Producer.Transaction.ttl, 3).
    setProperty(TSF_Dictionary.Producer.Transaction.keep_alive, 1).
    setProperty(TSF_Dictionary.Consumer.transaction_preload, 10).
    setProperty(TSF_Dictionary.Consumer.data_preload, 10)

  val producer = f.getProducer[String](
    name = "test_producer",
    txnGenerator = LocalGeneratorCreator.getGen(),
    converter = stringToArrayByteConverter,
    partitions = List(0,1,2),
    isLowPriority = false)

  val s1 = f.getSubscriber[String](
    name = "test_subscriber",
    txnGenerator = LocalGeneratorCreator.getGen(),
    converter = arrayByteToStringConverter,
    partitions = List(0,1,2),
    offset = Oldest,
    isUseLastOffset = true,
    callback = new BasicSubscriberCallback[String] {
      override def onEvent(
                            subscriber: BasicSubscribingConsumer[String],
                            partition: Int,
                            transactionUuid: UUID): Unit = {

      }})

  "Fire async checkpoint by producer and wait when complete" should "consumer read checkpoint from DB" in {

  }

}
