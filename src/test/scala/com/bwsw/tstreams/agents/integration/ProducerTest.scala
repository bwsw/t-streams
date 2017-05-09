package com.bwsw.tstreams.agents.integration

import com.bwsw.tstreams.agents.producer._
import com.bwsw.tstreams.env.ConfigurationOptions
import com.bwsw.tstreams.testutils._
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}


class ProducerTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  // keep it greater than 3
  val ALL_PARTITIONS = 10

  lazy val srv = TestStorageServer.get()
  lazy val storageClient = f.getStorageClient()

  lazy val producer = f.getProducer(
    name = "test_producer",
    partitions = (0 until ALL_PARTITIONS).toSet)


  override def beforeAll(): Unit = {
    System.setProperty("DEBUG", "true")
    System.setProperty("log4j.rootLogger", "DEBUG, STDOUT")
    System.setProperty("log4j.appender.STDOUT", "org.apache.log4j.ConsoleAppender")
    System.setProperty("log4j.appender.STDOUT.layout", "org.apache.log4j.PatternLayout")
    System.setProperty("log4j.appender.STDOUT.layout.ConversionPattern", "%5p [%t] (%F:%L) – %m%n")

    f.setProperty(ConfigurationOptions.Stream.name, "test_stream").
      setProperty(ConfigurationOptions.Stream.partitionsCount, ALL_PARTITIONS).
      setProperty(ConfigurationOptions.Stream.ttlSec, 60 * 10).
      setProperty(ConfigurationOptions.Coordination.connectionTimeoutMs, 7000).
      setProperty(ConfigurationOptions.Coordination.sessionTimeoutMs, 7000).
      setProperty(ConfigurationOptions.Producer.transportTimeoutMs, 5000).
      setProperty(ConfigurationOptions.Producer.Transaction.ttlMs, 6000).
      setProperty(ConfigurationOptions.Producer.Transaction.keepAliveMs, 2000).
      setProperty(ConfigurationOptions.Consumer.transactionPreload, 10).
      setProperty(ConfigurationOptions.Consumer.dataPreload, 10)

    srv
    storageClient.createStream("test_stream", 2, 24 * 3600, "")
    storageClient.shutdown()
  }

  "Producer.isMeAMasterOfPartition" should "return true" in {
    (0 until ALL_PARTITIONS).foreach(p => producer.isMasterOfPartition(p) shouldBe true)
  }

  "Producer.isMeAMasterOfPartition" should "return false" in {
    (ALL_PARTITIONS until ALL_PARTITIONS + 1).foreach(p => producer.isMasterOfPartition(p) shouldBe false)
  }

  "BasicProducer.newTransaction()" should "return BasicProducerTransaction instance" in {
    val transaction = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
    transaction.checkpoint()
    transaction.isInstanceOf[ProducerTransaction] shouldEqual true
  }

  "BasicProducer.newTransaction(ProducerPolicies.errorIfOpen)" should "throw exception if previous transaction was not closed" in {
    val transaction1 = producer.newTransaction(NewProducerTransactionPolicy.CheckpointIfOpened, 2)
    intercept[IllegalStateException] {
      producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, 2)
    }
    transaction1.checkpoint()
  }

  "BasicProducer.newTransaction(checkpointIfOpen)" should "not throw exception if previous transaction was not closed" in {
    producer.newTransaction(NewProducerTransactionPolicy.CheckpointIfOpened, 2)
    val transaction2 = producer.newTransaction(NewProducerTransactionPolicy.CheckpointIfOpened, 2)
    transaction2.checkpoint()
  }

  "BasicProducer.getTransaction()" should "return transaction reference if it was created or None" in {
    val transaction = producer.newTransaction(NewProducerTransactionPolicy.CheckpointIfOpened, 1)
    val transactionRef = producer.getOpenedTransactionForPartition(1)
    transaction.checkpoint()
    val checkVal = transactionRef.get == transaction
    checkVal shouldEqual true
  }

  "BasicProducer.instantTransaction" should "work well for reliable delivery" in {
    val data = Seq(new Array[Byte](128))
    producer.instantTransaction(0, data, isReliable = true) > 0 shouldBe true
  }

  "BasicProducer.instantTransaction" should "work well for unreliable delivery" in {
    val data = Seq(new Array[Byte](128))
    producer.instantTransaction(0, data, isReliable = false) > 0 shouldBe true
  }

  "BasicProducer.instantTransaction" should "work and doesn't prevent from correct functioning of regular one" in {
    val regularTransaction = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, 0)
    regularTransaction.send("test".getBytes)
    val data = Seq(new Array[Byte](128))
    producer.instantTransaction(0, data, isReliable = false) > 0 shouldBe true
    regularTransaction.checkpoint()
  }

  override def afterAll(): Unit = {
    producer.stop()
    TestStorageServer.dispose(srv)
    onAfterAll()
  }
}
