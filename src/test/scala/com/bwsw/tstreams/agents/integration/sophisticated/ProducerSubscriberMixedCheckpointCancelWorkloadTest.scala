package com.bwsw.tstreams.agents.integration.sophisticated

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.env.ConfigurationOptions
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * Created by Ivan Kudryavtsev on 14.04.17.
  */
class ProducerSubscriberMixedCheckpointCancelWorkloadTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  val TRANSACTION_COUNT = 10000
  val CANCEL_PROBABILITY = 0.05

  override def beforeAll(): Unit = {
    f.setProperty(ConfigurationOptions.Stream.name, "test_stream").
      setProperty(ConfigurationOptions.Stream.partitionsCount, 3).
      setProperty(ConfigurationOptions.Stream.ttlSec, 60 * 10).
      setProperty(ConfigurationOptions.Coordination.connectionTimeoutMs, 7000).
      setProperty(ConfigurationOptions.Coordination.sessionTimeoutMs, 7000).
      setProperty(ConfigurationOptions.Producer.transportTimeoutMs, 5000).
      setProperty(ConfigurationOptions.Producer.Transaction.ttlMs, 500).
      setProperty(ConfigurationOptions.Producer.Transaction.keepAliveMs, 100).
      setProperty(ConfigurationOptions.Consumer.transactionPreload, 500).
      setProperty(ConfigurationOptions.Consumer.dataPreload, 10)
  }

  def test(startBeforeProducerSend: Boolean = false) = {
    val producerAccumulator = ListBuffer[Long]()
    val subscriberAccumulator = ListBuffer[Long]()
    val subscriberLatch = new CountDownLatch(1)

    val srv = TestStorageServer.get()
    val storageClient = f.getStorageClient()

    if(storageClient.checkStreamExists("test_stream"))
      storageClient.deleteStream("test_stream")

    storageClient.createStream("test_stream", 1, 24 * 3600, "")
    storageClient.shutdown()

    val subscriber = f.getSubscriber(name = "sv2",
      partitions = Set(0),
      offset = Oldest,
      useLastOffset = false,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => this.synchronized {
        subscriberAccumulator.append(transaction.getTransactionID)
        if (producerAccumulator.size == subscriberAccumulator.size)
          subscriberLatch.countDown()
      })

    val producer = f.getProducer(
      name = "test_producer",
      partitions = Set(0))

    if (startBeforeProducerSend)
      subscriber.start()

    (0 until TRANSACTION_COUNT).foreach(_ => {
      val txn = producer.newTransaction()
      if (CANCEL_PROBABILITY < Random.nextDouble()) {
        txn.send("test".getBytes())
        txn.checkpoint()
        producerAccumulator.append(txn.getTransactionID)
      } else {
        txn.cancel()
      }
    })
    producer.stop()

    if (!startBeforeProducerSend)
      subscriber.start()

    subscriberLatch.await(100, TimeUnit.SECONDS) shouldBe true

    producerAccumulator shouldBe subscriberAccumulator

    subscriber.stop()

    TestStorageServer.dispose(srv)

  }

  it should "accumulate only checkpointed transactions, subscriber starts after" in {
    test(startBeforeProducerSend = false)
  }

  it should "accumulate only checkpointed transactions, subscriber starts before" in {
    test(startBeforeProducerSend = true)
  }

  override def afterAll(): Unit = {
    onAfterAll()
  }
}
