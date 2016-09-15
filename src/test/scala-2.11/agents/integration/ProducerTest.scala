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
  System.setProperty("log4j.appender.STDOUT", "org.apache.log4j.ConsoleAppender")
  System.setProperty("log4j.appender.STDOUT.layout", "org.apache.log4j.PatternLayout")
  System.setProperty("log4j.appender.STDOUT.layout.ConversionPattern", "%5p [%t] (%F:%L) – %m%n")

  // keep it greater than 3
  val ALL_PARTITIONS = 10

  f.setProperty(TSF_Dictionary.Stream.NAME, "test_stream").
    setProperty(TSF_Dictionary.Stream.PARTITIONS, ALL_PARTITIONS).
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
    transactionGenerator = LocalGeneratorCreator.getGen(),
    converter = stringToArrayByteConverter,
    partitions = (0 until ALL_PARTITIONS).toSet,
    isLowPriority = false)

  "Producer.isMeAMasterOfPartition" should "return true" in {
    (0 until ALL_PARTITIONS).foreach(p => producer.isMeAMasterOfPartition(p) shouldBe true)
  }

  "Producer.isMeAMasterOfPartition" should "return false" in {
    (ALL_PARTITIONS until ALL_PARTITIONS + 1).foreach(p => producer.isMeAMasterOfPartition(p) shouldBe false)
  }

  "BasicProducer.newTransaction()" should "return BasicProducerTransaction instance" in {
    val transaction = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    transaction.checkpoint()
    transaction.isInstanceOf[ProducerTransaction[_]] shouldEqual true
  }

  "BasicProducer.newTransaction(ProducerPolicies.errorIfOpen)" should "throw exception if previous transaction was not closed" in {
    val transaction1 = producer.newTransaction(NewTransactionProducerPolicy.CheckpointIfOpened, 2)
    intercept[IllegalStateException] {
      producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened, 2)
    }
    transaction1.checkpoint()
  }

  "BasicProducer.newTransaction(checkpointIfOpen)" should "not throw exception if previous transaction was not closed" in {
    producer.newTransaction(NewTransactionProducerPolicy.CheckpointIfOpened, 2)
    val transaction2 = producer.newTransaction(NewTransactionProducerPolicy.CheckpointIfOpened, 2)
    transaction2.checkpoint()
  }

  "BasicProducer.getTransaction()" should "return transaction reference if it was created or None" in {
    val transaction = producer.newTransaction(NewTransactionProducerPolicy.CheckpointIfOpened, 1)
    val transactionRef = producer.getOpenedTransactionForPartition(1)
    transaction.checkpoint()
    val checkVal = transactionRef.get == transaction
    checkVal shouldEqual true
  }

  override def afterAll(): Unit = {
    producer.stop()
    onAfterAll()
  }
}
