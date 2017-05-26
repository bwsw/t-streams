package com.bwsw.tstreams.agents.integration

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.Offset.Newest
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.env.ConfigurationOptions
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 25.05.17.
  */
class ProducerCancelPartitionTest  extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  lazy val srv = TestStorageServer.getNewClean()

  override def beforeAll(): Unit = {
    srv
    createNewStream()
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(srv)
    onAfterAll()
  }

  it should "cancel transactions for a partition and read next ones without a delay" in {
    val TOTAL_CANCEL = 100
    val TOTAL_CHECKPOINT = 100
    val latch = new CountDownLatch(TOTAL_CHECKPOINT)

    val producer = f.getProducer(
      name = "test_producer",
      partitions = Set(0))

    val subscriber = f.getSubscriber(name = "subscriber",
      partitions = Set(0),
      offset = Newest,
      useLastOffset = false,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => latch.countDown())
    subscriber.start()

    producer.cancel(0) // cancel empty one

    (0 until TOTAL_CANCEL).foreach(t => producer.newTransaction())
    producer.cancel(0)

    (0 until TOTAL_CHECKPOINT).foreach(t => producer.newTransaction().send("test"))
    producer.checkpoint(0)

    latch.await(f.getProperty(ConfigurationOptions.Producer.Transaction.ttlMs).asInstanceOf[Int] / 2, TimeUnit.MILLISECONDS) shouldBe true
    producer.stop()
    subscriber.stop()
  }

}
