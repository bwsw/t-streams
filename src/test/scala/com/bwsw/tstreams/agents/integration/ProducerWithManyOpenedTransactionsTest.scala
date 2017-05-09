package com.bwsw.tstreams.agents.integration

import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.producer._
import com.bwsw.tstreams.env.ConfigurationOptions
import com.bwsw.tstreams.testutils._
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}


class ProducerWithManyOpenedTransactionsTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  lazy val srv = TestStorageServer.get()
  lazy val storageClient = f.getStorageClient()

  lazy val producer = f.getProducer(
    name = "test_producer",
    partitions = Set(0, 1, 2))

  lazy val consumer = f.getConsumer(
    name = "test_consumer",
    partitions = Set(0, 1, 2),
    offset = Oldest,
    useLastOffset = true)

  override def beforeAll(): Unit = {
    f.setProperty(ConfigurationOptions.Stream.name, "test_stream").
      setProperty(ConfigurationOptions.Stream.partitionsCount, 3).
      setProperty(ConfigurationOptions.Stream.ttlSec, 60 * 10).
      setProperty(ConfigurationOptions.Coordination.connectionTimeoutMs, 7000).
      setProperty(ConfigurationOptions.Coordination.sessionTimeoutMs, 7000).
      setProperty(ConfigurationOptions.Producer.transportTimeoutMs, 5000).
      setProperty(ConfigurationOptions.Producer.Transaction.ttlMs, 6000).
      setProperty(ConfigurationOptions.Producer.Transaction.keepAliveMs, 2000).
      setProperty(ConfigurationOptions.Consumer.transactionPreload, 10).
      setProperty(ConfigurationOptions.Consumer.dataPreload, 10)


    srv
    storageClient.createStream("test_stream", 3, 24 * 3600, "")
    storageClient.shutdown()

    consumer.start
  }


  "BasicProducer.newTransaction()" should "return BasicProducerTransaction instance" in {
    val data1 = (for (i <- 0 until 10) yield randomKeyspace).sorted
    val data2 = (for (i <- 0 until 10) yield randomKeyspace).sorted
    val data3 = (for (i <- 0 until 10) yield randomKeyspace).sorted


    val transaction1: ProducerTransaction = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
    val transaction2: ProducerTransaction = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
    val transaction3: ProducerTransaction = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)

    val l = new CountDownLatch(1)
    srv.notifyProducerTransactionCompleted(t => t.transactionID == transaction1.getTransactionID() && t.state == TransactionStates.Checkpointed, l.countDown())

    data1.foreach(x => transaction1.send(x))
    data2.foreach(x => transaction2.send(x))
    data3.foreach(x => transaction3.send(x))

    transaction3.checkpoint()
    transaction2.checkpoint()
    transaction1.checkpoint()

    l.await()

    consumer.getTransaction(0).get.getAll().map(i => new String(i)).sorted shouldBe data1
    consumer.getTransaction(1).get.getAll().map(i => new String(i)).sorted shouldBe data2
    consumer.getTransaction(2).get.getAll().map(i => new String(i)).sorted shouldBe data3

    (0 to 2).foreach(p => consumer.getTransaction(p).isEmpty shouldBe true)
  }

  "BasicProducer.newTransaction()" should "return error if try to open more than 3 transactions for 3 partitions if ProducerPolicies.errorIfOpened" in {
    producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
    producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
    producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)

    val r: Boolean = try {
      producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
      false
    } catch {
      case e: IllegalStateException => true
    }
    r shouldBe true
    producer.checkpoint()
  }

  "BasicProducer.newTransaction()" should "return ok if try to open more than 3 transactions for 3 partitions if ProducerPolicies.checkpointIfOpened" in {
    producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
    producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
    producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)

    val r: Boolean = try {
      producer.newTransaction(NewProducerTransactionPolicy.CheckpointIfOpened)
      true
    } catch {
      case e: IllegalStateException => false
    }
    r shouldBe true
    producer.checkpoint()
  }

  "BasicProducer.newTransaction()" should "return ok if try to open more than 3 transactions for 3 partitions if ProducerPolicies.cancelIfOpened" in {
    producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
    producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)
    producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)

    val r: Boolean = try {
      producer.newTransaction(NewProducerTransactionPolicy.CancelIfOpened)
      true
    } catch {
      case e: IllegalStateException => false
    }
    r shouldBe true
    producer.checkpoint()
  }

  override def afterAll(): Unit = {
    producer.stop()
    consumer.stop()
    TestStorageServer.dispose(srv)
    onAfterAll()
  }
}
