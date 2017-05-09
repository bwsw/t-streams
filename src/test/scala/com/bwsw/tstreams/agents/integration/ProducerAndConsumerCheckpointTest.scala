package com.bwsw.tstreams.agents.integration

import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.agents.consumer.ConsumerTransaction
import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.producer.NewProducerTransactionPolicy
import com.bwsw.tstreams.common.TimeTracker
import com.bwsw.tstreams.env.ConfigurationOptions
import com.bwsw.tstreams.testutils._
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}


class ProducerAndConsumerCheckpointTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  lazy val srv = TestStorageServer.get()
  lazy val storageClient = f.getStorageClient()

  lazy val producer = f.getProducer(
    name = "test_producer",
    partitions = Set(0))

  lazy val consumer = f.getConsumer(
    name = "test_consumer",
    partitions = Set(0),
    offset = Oldest,
    useLastOffset = true)

  lazy val consumer2 = f.getConsumer(
    name = "test_consumer",
    partitions = Set(0),
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
      setProperty(ConfigurationOptions.Consumer.dataPreload, 10).
      setProperty(ConfigurationOptions.Producer.Transaction.batchSize, 100)

    srv
    storageClient.createStream("test_stream", 3, 24 * 3600, "")
    storageClient.shutdown()
  }

  "producer, consumer" should "producer - generate many transactions, consumer - retrieve all of them with reinitialization after some time" in {
    val TRANSACTIONS_COUNT = 10000
    val dataToSend = (for (i <- 0 until 1) yield randomKeyspace).sorted

    var counter = 0

    val l = new CountDownLatch(1)

    (0 until TRANSACTIONS_COUNT) foreach { _ =>
      TimeTracker.update_start("Overall")
      TimeTracker.update_start("newTransaction")
      val transaction = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened)

      counter += 1
      if (counter == TRANSACTIONS_COUNT)
        srv.notifyProducerTransactionCompleted(t => t.transactionID == transaction.getTransactionID() && t.state == TransactionStates.Checkpointed, l.countDown())

      TimeTracker.update_end("newTransaction")

      TimeTracker.update_start("Send")
      dataToSend foreach { part => transaction.send(part.getBytes()) }
      TimeTracker.update_end("Send")

      TimeTracker.update_start("checkpoint")
      transaction.checkpoint()
      TimeTracker.update_end("checkpoint")
      TimeTracker.update_end("Overall")
    }

    val firstPart = TRANSACTIONS_COUNT / 3
    val secondPart = TRANSACTIONS_COUNT - firstPart

    l.await()

    val l1 = new CountDownLatch(1)

    consumer.start
    (0 until firstPart) foreach { counter =>
      val transaction: ConsumerTransaction = consumer.getTransaction(0).get

      if (counter == firstPart - 1)
        srv.notifyConsumerTransactionCompleted(ct => transaction.getTransactionID() == ct.transactionID, l1.countDown())

      transaction.getAll().map(i => new String(i)).sorted shouldBe dataToSend
      consumer.checkpoint()
    }

    l1.await()

    consumer2.start
    (0 until secondPart) foreach { _ =>
      val transaction: ConsumerTransaction = consumer2.getTransaction(0).get
      transaction.getAll().map(i => new String(i)).sorted shouldBe dataToSend
    }

    //assert that is nothing to read
    (0 until Integer.parseInt(f.getProperty(ConfigurationOptions.Stream.partitionsCount).toString)) foreach { _ =>
      consumer2.getTransaction(0).isEmpty shouldBe true
    }
  }

  override def afterAll(): Unit = {
    producer.stop()
    Seq(consumer, consumer2).foreach(c => c.stop())
    onAfterAll()
    TestStorageServer.dispose(srv)
    TimeTracker.dump()
  }
}