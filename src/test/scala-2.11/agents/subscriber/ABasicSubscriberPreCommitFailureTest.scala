package agents.subscriber

import java.util.UUID
import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.consumer.subscriber.{Callback, SubscribingConsumer}
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.debug.GlobalHooks
import com.bwsw.tstreams.env.TSF_Dictionary
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{LocalGeneratorCreator, TestUtils}

class ABasicSubscriberPreCommitFailureTest extends FlatSpec with Matchers
  with BeforeAndAfterAll with TestUtils {

  System.setProperty("DEBUG", "true")
  GlobalHooks.addHook(GlobalHooks.preCommitFailure, () => throw new RuntimeException)

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

  val lock = new ReentrantLock()
  var acc = 0
  val totalTxns = 30
  val dataInTxn = 10
  val data = randomString

  val producer = f.getProducer[String](
    name = "test_producer",
    txnGenerator = LocalGeneratorCreator.getGen(),
    converter = stringToArrayByteConverter,
    partitions = List(0,1,2),
    isLowPriority = false)

  val callback = new Callback[String] {
    override def onEvent(
                          subscriber: SubscribingConsumer[String],
                          partition: Int,
                          transactionUuid: UUID): Unit = {
      lock.lock()

      acc += 1

      subscriber.setLocalOffset(partition, transactionUuid)
      subscriber.checkpoint()
      lock.unlock()
    }
  }

  val s1 = f.getSubscriber[String](
    name = "test_subscriber",
    txnGenerator = LocalGeneratorCreator.getGen(),
    converter = arrayByteToStringConverter,
    partitions = List(0,1,2),
    callback = callback,
    offset = Oldest,
    isUseLastOffset = true)

  val s2 = f.getSubscriber[String](
    name = "test_subscriber",
    txnGenerator = LocalGeneratorCreator.getGen(),
    converter = arrayByteToStringConverter,
    partitions = List(0,1,2),
    callback = callback,
    offset = Oldest,
    isUseLastOffset = true)

  "subscribe consumer" should "retrieve all sent messages" in {

    s1.start()
    sendTxnsAndWait(totalTxns, dataInTxn, data)
    s1.stop()

    s2.start()
    sendTxnsAndWait(totalTxns, dataInTxn, data)
    s2.stop()

    acc shouldEqual 0
  }

  def sendTxnsAndWait(totalTxns: Int, dataInTxn: Int, data: String) = {
    (0 until totalTxns) foreach { x =>
      val txn = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
      (0 until dataInTxn) foreach { _ =>
        txn.send(data)
      }
      try {
        txn.checkpoint()
      } catch {
        case e: RuntimeException =>
      }
    }
  }

  override def afterAll(): Unit = {
    System.clearProperty("DEBUG")
    GlobalHooks.clear()
    producer.stop()
    onAfterAll()
  }
}
