package agents.integration

import com.bwsw.tstreams.agents.producer._
import com.bwsw.tstreams.env.TSF_Dictionary
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._


class ProducerTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "debug")
  System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "DEBUG")
  System.setProperty("DEBUG", "true")
  System.setProperty("log4j.rootLogger", "DEBUG, STDOUT")
  System.setProperty("log4j.appender.STDOUT","org.apache.log4j.ConsoleAppender")
  System.setProperty("log4j.appender.STDOUT.layout","org.apache.log4j.PatternLayout")
  System.setProperty("log4j.appender.STDOUT.layout.ConversionPattern","%5p [%t] (%F:%L) – %m%n")

  f.setProperty(TSF_Dictionary.Stream.NAME,"test_stream").
    setProperty(TSF_Dictionary.Stream.PARTITIONS,1000).
    setProperty(TSF_Dictionary.Stream.TTL, 60 * 10).
    setProperty(TSF_Dictionary.Coordination.CONNECTION_TIMEOUT, 7).
    setProperty(TSF_Dictionary.Coordination.TTL, 7).
    setProperty(TSF_Dictionary.Producer.TRANSPORT_TIMEOUT, 5).
    setProperty(TSF_Dictionary.Producer.Transaction.TTL, 6).
    setProperty(TSF_Dictionary.Producer.Transaction.KEEP_ALIVE, 2).
    setProperty(TSF_Dictionary.Consumer.TRANSACTION_PRELOAD, 10).
    setProperty(TSF_Dictionary.Consumer.DATA_PRELOAD, 10)

  val producer = f.getProducer[String](
    name = "test_producer",
    txnGenerator = LocalGeneratorCreator.getGen(),
    converter = stringToArrayByteConverter,
    partitions = (0 until 1000).toSet,
    isLowPriority = false)

  "BasicProducer.newTransaction()" should "return BasicProducerTransaction instance" in {

    val txn: Transaction[String] = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    txn.checkpoint()
    txn.isInstanceOf[Transaction[_]] shouldEqual true
  }

  "BasicProducer.newTransaction(ProducerPolicies.errorIfOpen)" should "throw exception if previous transaction was not closed" in {
    val txn1: Transaction[String] = producer.newTransaction(NewTransactionProducerPolicy.CheckpointIfOpened, 2)
    intercept[IllegalStateException] {
      producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened, 2)
    }
    txn1.checkpoint()
  }

  "BasicProducer.newTransaction(checkpointIfOpen)" should "not throw exception if previous transaction was not closed" in {
    producer.newTransaction(NewTransactionProducerPolicy.CheckpointIfOpened, 2)
    val txn2 = producer.newTransaction(NewTransactionProducerPolicy.CheckpointIfOpened, 2)
    txn2.checkpoint()
  }

  "BasicProducer.getTransaction()" should "return transaction reference if it was created or None" in {
    val txn = producer.newTransaction(NewTransactionProducerPolicy.CheckpointIfOpened, 1)
    val txnRef = producer.getOpenedTransactionForPartition(1)
    txn.checkpoint()
    val checkVal = txnRef.get == txn
    checkVal shouldEqual true
  }

  override def afterAll(): Unit = {
    producer.stop()
    onAfterAll()
  }
}
