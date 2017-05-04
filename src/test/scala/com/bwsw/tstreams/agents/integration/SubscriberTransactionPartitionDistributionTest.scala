package com.bwsw.tstreams.agents.integration

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.Offset.Newest
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.env.ConfigurationOptions
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer

/**
  * Created by Ivan Kudryavtsev on 14.04.17.
  */
class SubscriberTransactionPartitionDistributionTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  val ALL_PARTITIONS = 4
  val TRANSACTION_COUNT = 1000

  lazy val srv = TestStorageServer.get()
  lazy val storageClient = f.getStorageClient()

  override def beforeAll(): Unit = {
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
    storageClient.createStream("test_stream", ALL_PARTITIONS, 24 * 3600, "")
    storageClient.shutdown()
  }

  it should "ensure that transactions are distributed to the same partitions on producer and subscriber" in {
    val accumulatorBuilder = () => (0 until ALL_PARTITIONS).map(_ => ListBuffer[Long]()).toArray
    val producerTransactionsAccumulator = accumulatorBuilder()
    val subscriberTransactionsAccumulator = accumulatorBuilder()
    var counter = 0
    val subscriberLatch = new CountDownLatch(1)

    val producer = f.getProducer(
      name = "test_producer",
      partitions = (0 until ALL_PARTITIONS).toSet)

    val subscriber = f.getSubscriber(name = "sv2",
      partitions = (0 until ALL_PARTITIONS).toSet,
      offset = Newest,
      useLastOffset = false,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => this.synchronized {
        subscriberTransactionsAccumulator(transaction.getPartition()).append(transaction.getTransactionID())
        counter += 1
        if (counter == TRANSACTION_COUNT)
          subscriberLatch.countDown()
      })
    subscriber.start()
    (0 until TRANSACTION_COUNT).foreach(_ => {
      val t = producer.newTransaction()
      t.send("test".getBytes())
      t.checkpoint()
      producerTransactionsAccumulator(t.getPartition()).append(t.getTransactionID())
    })

    subscriberLatch.await(30, TimeUnit.SECONDS) shouldBe true
    (0 until ALL_PARTITIONS).foreach(partition =>
      subscriberTransactionsAccumulator(partition) shouldBe producerTransactionsAccumulator(partition))

    subscriber.stop()
    producer.stop()
  }

  override def afterAll(): Unit = {
    onAfterAll()
    TestStorageServer.dispose(srv)
  }
}
