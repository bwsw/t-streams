package com.bwsw.tstreams.agents.integration

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.Offset.Newest
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.env.ConfigurationOptions
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 26.05.17.
  */
class CheckpointGroupQueuedTransactionsCheckpointCancelTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils  {
  lazy val srv = TestStorageServer.getNewClean()

  override def beforeAll(): Unit = {
    srv
    createNewStream()
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(srv)
    onAfterAll()
  }

  it should "cancel queued producer events properly" in {
    val TOTAL_CANCEL = 100
    val TOTAL_CHECKPOINT = 100
    val latch = new CountDownLatch(TOTAL_CHECKPOINT)
    val group = f.getCheckpointGroup()

    val producer = f.getProducer(
      name = "test_producer",
      partitions = Set(0))

    group.add(producer)

    val subscriber = f.getSubscriber(name = "ss+2",
      partitions = Set(0),
      offset = Newest,
      useLastOffset = true,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => latch.countDown())
    subscriber.start()

    group.cancel()
    group.checkpoint()

    (0 until TOTAL_CANCEL).foreach(t => producer.newTransaction())
    group.cancel()

    (0 until TOTAL_CHECKPOINT).foreach(t => producer.newTransaction().send("test"))
    group.checkpoint()

    latch.await(f.getProperty(ConfigurationOptions.Producer.Transaction.ttlMs).asInstanceOf[Int] / 2, TimeUnit.MILLISECONDS) shouldBe true

    subscriber.stop()
    producer.stop()
  }
}
