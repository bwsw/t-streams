package agents.integration

import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.debug.GlobalHooks
import com.bwsw.tstreams.env.TSF_Dictionary
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{LocalGeneratorCreator, TestUtils}

/**
  * Created by Ivan Kudryavtsev on 02.08.16.
  */
class AsynchronousTransactionTests extends FlatSpec with Matchers
  with BeforeAndAfterAll with TestUtils {

  // required for hooks to work
  System.setProperty("DEBUG", "true")
  System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "DEBUG")

  f.setProperty(TSF_Dictionary.Stream.NAME, "test_stream").
    setProperty(TSF_Dictionary.Stream.PARTITIONS, 3).
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
    transactionGenerator = LocalGeneratorCreator.getGen(),
    converter = stringToArrayByteConverter,
    partitions = Set(0),
    isLowPriority = false)

  "Fire async checkpoint by producer and wait when complete" should "consumer get transaction from DB" in {
    val l = new CountDownLatch(1)
    GlobalHooks.addHook(GlobalHooks.afterCommitFailure, () => {
      l.countDown()
    })

    val c = f.getConsumer[String](
      name = "test_subscriber",
      transactionGenerator = LocalGeneratorCreator.getGen(),
      converter = arrayByteToStringConverter,
      partitions = Set(0),
      offset = Oldest,
      isUseLastOffset = true)

    val pTransaction = producer.newTransaction(policy = NewTransactionProducerPolicy.ErrorIfOpened)
    pTransaction.send("test")
    pTransaction.checkpoint(isSynchronous = false)
    l.await()
    c.start()
    val cTransaction = c.getTransaction(0)

    cTransaction.isDefined shouldBe true
    pTransaction.getTransactionUUID shouldBe cTransaction.get.getTransactionUUID
    if (cTransaction.isDefined)
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
      transactionGenerator = LocalGeneratorCreator.getGen(),
      converter = arrayByteToStringConverter,
      partitions = Set(0),
      offset = Oldest,
      isUseLastOffset = true)

    val pTransaction = producer.newTransaction(policy = NewTransactionProducerPolicy.ErrorIfOpened)
    pTransaction.send("test")
    pTransaction.checkpoint(isSynchronous = false)
    l.await()
    c.start()
    val cTransaction = c.getTransaction(0)

    cTransaction.isDefined shouldBe false
  }


  "Fire async checkpoint by producer (with pre delay) and wait when complete" should "consumer not get transaction from DB" in {
    val l = new CountDownLatch(1)
    GlobalHooks.addHook(GlobalHooks.preCommitFailure, () => {
      l.await()
      throw new Exception("expected")
    })

    val c = f.getConsumer[String](
      name = "test_subscriber",
      transactionGenerator = LocalGeneratorCreator.getGen(),
      converter = arrayByteToStringConverter,
      partitions = Set(0),
      offset = Oldest,
      isUseLastOffset = true)

    val pTransaction = producer.newTransaction(policy = NewTransactionProducerPolicy.ErrorIfOpened)
    pTransaction.send("test")
    pTransaction.checkpoint(isSynchronous = false)
    c.start()
    val cTransaction = c.getTransaction(0)
    l.countDown()
    cTransaction.isDefined shouldBe false
  }

  override def afterAll() = {
    producer.stop()
    System.setProperty("DEBUG", "false")
    onAfterAll()
  }
}
