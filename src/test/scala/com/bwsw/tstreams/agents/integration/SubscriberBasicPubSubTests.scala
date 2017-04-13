package com.bwsw.tstreams.agents.integration

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.Offset.{Newest, Oldest}
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.env.ConfigurationOptions
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import com.bwsw.tstreamstransactionserver.netty.server.Server
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 26.08.16.
  */
class SubscriberBasicPubSubTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  f.setProperty(ConfigurationOptions.Stream.name, "test_stream").
    setProperty(ConfigurationOptions.Stream.partitionsCount, 3).
    setProperty(ConfigurationOptions.Stream.ttlSec, 60 * 10).
    setProperty(ConfigurationOptions.Coordination.connectionTimeoutMs, 7000).
    setProperty(ConfigurationOptions.Coordination.sessionTimeoutMs, 7000).
    setProperty(ConfigurationOptions.Producer.transportTimeoutMs, 5000).
    setProperty(ConfigurationOptions.Producer.Transaction.ttlMs, 6000).
    setProperty(ConfigurationOptions.Producer.Transaction.keepAliveMs, 2000).
    setProperty(ConfigurationOptions.Consumer.transactionPreload, 500).
    setProperty(ConfigurationOptions.Consumer.dataPreload, 10)

  var srv: Server = _
  srv = TestStorageServer.get()
  val storageClient = f.getStorageClient()
  storageClient.createStream("test_stream", 3, 24 * 3600, "")
  storageClient.shutdown()

  it should "handle all transactions produced by producer" in {

    val TOTAL = 1000
    val latch = new CountDownLatch(1)

    var subscriberTransactionsAmount = 0
    val producer = f.getProducer(
      name = "test_producer",
      partitions = Set(0, 1, 2))

    val s = f.getSubscriber(name = "sv2",
      partitions = Set(0, 1, 2),
      offset = Oldest,
      useLastOffset = false,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => this.synchronized {
        subscriberTransactionsAmount += 1
        transaction.getAll()
        if (subscriberTransactionsAmount == TOTAL)
          latch.countDown()
      })
    s.start()
    for (it <- 0 until TOTAL) {
      val transaction = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
      transaction.send("test")
      transaction.checkpoint()
    }
    producer.stop()
    latch.await(60, TimeUnit.SECONDS) shouldBe true
    s.stop()
    subscriberTransactionsAmount shouldBe TOTAL
  }

  it should "handle all transactions produced by two different producers" in {

    val TOTAL = 1000
    var subscriberTransactionsAmount = 0
    val latch = new CountDownLatch(1)

    val producer1 = f.getProducer(
      name = "test_producer",
      partitions = Set(0, 1, 2))

    val s = f.getSubscriber(name = "sv2",
      partitions = Set(0, 1, 2),
      offset = Newest,
      useLastOffset = false,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => this.synchronized {
        subscriberTransactionsAmount += 1
        if (subscriberTransactionsAmount == TOTAL * 2)
          latch.countDown()
      })
    s.start()
    for (it <- 0 until TOTAL) {
      val transaction = producer1.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
      transaction.send("test")
      transaction.checkpoint()
    }
    producer1.stop()
    val producer2 = f.getProducer(
      name = "test_producer2",
      partitions = Set(0, 1, 2))

    for (it <- 0 until TOTAL) {
      val transaction = producer2.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
      transaction.send("test")
      transaction.checkpoint()
    }
    producer2.stop()
    latch.await(10, TimeUnit.SECONDS) shouldBe true
    s.stop()
    subscriberTransactionsAmount shouldBe TOTAL * 2
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(srv)
    onAfterAll()
  }
}
