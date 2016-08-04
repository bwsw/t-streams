package agents.subscriber

import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.agents.consumer.Offsets.Oldest
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.debug.GlobalHooks
import com.bwsw.tstreams.env.TSF_Dictionary
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{LocalGeneratorCreator, TestUtils}

/**
  * Created by ivan on 02.08.16.
  */
class AsynchronousTransactionTests  extends FlatSpec with Matchers
  with BeforeAndAfterAll with TestUtils {

  // required for hooks to work
  System.setProperty("DEBUG", "true")
  System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "DEBUG");

  f.setProperty(TSF_Dictionary.Stream.NAME,"test_stream").
    setProperty(TSF_Dictionary.Stream.PARTITIONS,3).
    setProperty(TSF_Dictionary.Stream.TTL, 60 * 10).
    setProperty(TSF_Dictionary.Coordination.CONNECTION_TIMEOUT, 7).
    setProperty(TSF_Dictionary.Coordination.TTL, 7).
    setProperty(TSF_Dictionary.Producer.MASTER_TIMEOUT, 5).
    setProperty(TSF_Dictionary.Producer.Transaction.TTL, 3).
    setProperty(TSF_Dictionary.Producer.Transaction.KEEP_ALIVE, 1).
    setProperty(TSF_Dictionary.Consumer.TRANSACTION_PRELOAD, 10).
    setProperty(TSF_Dictionary.Consumer.DATA_PRELOAD, 10)


  val producer = f.getProducer[String](
    name = "test_producer",
    txnGenerator = LocalGeneratorCreator.getGen(),
    converter = stringToArrayByteConverter,
    partitions = List(0),
    isLowPriority = false)

  "Fire async checkpoint by producer and wait when complete" should "consumer get transaction from DB" in {
    val l = new CountDownLatch(1)
    GlobalHooks.addHook(GlobalHooks.afterCommitFailure, () => {
      l.countDown()
    })

    val c = f.getConsumer[String](
      name = "test_subscriber",
      txnGenerator = LocalGeneratorCreator.getGen(),
      converter = arrayByteToStringConverter,
      partitions = List(0),
      offset = Oldest,
      isUseLastOffset = true)

    val ptxn = producer.newTransaction(policy = NewTransactionProducerPolicy.ErrorIfOpened)
    ptxn.send("test")
    ptxn.checkpoint(isSynchronous = false)
    l.await()
    c.start()
    val ctxn = c.getTransaction

    ctxn.isDefined shouldBe true
    ptxn.getTxnUUID shouldBe ctxn.get.getTxnUUID
    if(ctxn.isDefined)
      c.checkpoint()
  }

  "Fire async checkpoint by producer (with exception) and wait when complete" should "consumer not get transaction from DB" in {
    val l = new CountDownLatch(1)
    GlobalHooks.addHook(GlobalHooks.preCommitFailure, () => {
      l.countDown()
      throw new Exception("expected")
    })

    val c = f.getConsumer[String](
      name = "test_subscriber",
      txnGenerator = LocalGeneratorCreator.getGen(),
      converter = arrayByteToStringConverter,
      partitions = List(0),
      offset = Oldest,
      isUseLastOffset = true)

    val ptxn = producer.newTransaction(policy = NewTransactionProducerPolicy.ErrorIfOpened)
    ptxn.send("test")
    ptxn.checkpoint(isSynchronous = false)
    l.await()
    c.start()
    val ctxn = c.getTransaction

    ctxn.isDefined shouldBe false
  }


  "Fire async checkpoint by producer (with pre delay) and wait when complete" should "consumer not get transaction from DB" in {
    val l = new CountDownLatch(1)
    GlobalHooks.addHook(GlobalHooks.preCommitFailure, () => {
      l.await()
      throw new Exception("expected")
    })

    val c = f.getConsumer[String](
      name = "test_subscriber",
      txnGenerator = LocalGeneratorCreator.getGen(),
      converter = arrayByteToStringConverter,
      partitions = List(0),
      offset = Oldest,
      isUseLastOffset = true)

    val ptxn = producer.newTransaction(policy = NewTransactionProducerPolicy.ErrorIfOpened)
    ptxn.send("test")
    ptxn.checkpoint(isSynchronous = false)
    c.start()
    val ctxn = c.getTransaction
    l.countDown()
    ctxn.isDefined shouldBe false
  }

  override def afterAll() = {
    producer.stop()
    System.setProperty("DEBUG", "false")
    onAfterAll()
  }
}
