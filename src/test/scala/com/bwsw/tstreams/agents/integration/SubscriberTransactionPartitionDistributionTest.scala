package com.bwsw.tstreams.agents.integration

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.Offset.Newest
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer

/**
  * Created by Ivan Kudryavtsev on 14.04.17.
  */
class SubscriberTransactionPartitionDistributionTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  val PARTITIONS_COUNT = 4

  lazy val srv = TestStorageServer.getNewClean()

  override def beforeAll(): Unit = {
    srv
    createNewStream(partitions = PARTITIONS_COUNT)
  }

  it should "ensure that transactions are distributed to the same partitions on producer and subscriber" in {
    val TRANSACTION_COUNT = 1000

    val accumulatorBuilder = () => (0 until PARTITIONS_COUNT).map(_ => ListBuffer[Long]()).toArray
    val producerTransactionsAccumulator = accumulatorBuilder()
    val subscriberTransactionsAccumulator = accumulatorBuilder()
    var counter = 0
    val subscriberLatch = new CountDownLatch(1)

    val producer = f.getProducer(
      name = "test_producer",
      partitions = (0 until PARTITIONS_COUNT).toSet)

    val subscriber = f.getSubscriber(name = "sv2",
      partitions = (0 until PARTITIONS_COUNT).toSet,
      offset = Newest,
      useLastOffset = false,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => this.synchronized {
        subscriberTransactionsAccumulator(transaction.getPartition).append(transaction.getTransactionID)
        counter += 1
        if (counter == TRANSACTION_COUNT)
          subscriberLatch.countDown()
      })
    subscriber.start()
    (0 until TRANSACTION_COUNT).foreach(_ => {
      val t = producer.newTransaction()
      t.send("test".getBytes())
      t.checkpoint()
      producerTransactionsAccumulator(t.getPartition).append(t.getTransactionID)
    })

    subscriberLatch.await(30, TimeUnit.SECONDS) shouldBe true
    (0 until PARTITIONS_COUNT).foreach(partition =>
      subscriberTransactionsAccumulator(partition) shouldBe producerTransactionsAccumulator(partition))

    subscriber.stop()
    producer.stop()
  }

  override def afterAll(): Unit = {
    onAfterAll()
    TestStorageServer.dispose(srv)
  }
}
