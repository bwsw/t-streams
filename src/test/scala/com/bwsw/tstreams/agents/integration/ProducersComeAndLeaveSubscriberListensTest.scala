package com.bwsw.tstreams.agents.integration

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.env.ConfigurationOptions
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 13.04.17.
  */
class ProducersComeAndLeaveSubscriberListensTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
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

  val srv = TestStorageServer.get()
  val storageClient = f.getStorageClient()
  storageClient.createStream("test_stream", 3, 24 * 3600, "")
  storageClient.shutdown()

  it should "handle all transactions and do not loose them" in {
    val TRANSACTIONS_PER_PRODUCER = 100
    val PRODUCERS = 10
    val TXNS_PER_SEC = 100

    val latch = new CountDownLatch(PRODUCERS * TRANSACTIONS_PER_PRODUCER)

    val subscriber = f.getSubscriber(name = "sv2",
      partitions = Set(0, 1, 2),
      offset = Oldest,
      useLastOffset = false,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => latch.countDown())

    subscriber.start()

    (0 until PRODUCERS).foreach(_ => {
      val producer = f.getProducer(
        name = "test_producer",
        partitions = Set(0, 1, 2))
      (0 until TRANSACTIONS_PER_PRODUCER).foreach(_ => {
        val transaction = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
        transaction.send("test")
        transaction.checkpoint()
      })
      producer.stop()
    })

    latch.await(100 + TRANSACTIONS_PER_PRODUCER * PRODUCERS / TXNS_PER_SEC, TimeUnit.SECONDS)
    subscriber.stop()
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(srv)
    onAfterAll()
  }

}
