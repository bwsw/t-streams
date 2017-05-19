package com.bwsw.tstreams.agents.integration

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.env.ConfigurationOptions
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.util.Try

/**
  * Created by Ivan Kudryavtsev on 19.05.17.
  */
class StorageBlinkingTest  extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  override def beforeAll(): Unit = {
    val srv = TestStorageServer.getNewClean()
    createNewStream()
    TestStorageServer.dispose(srv)
  }

  override def afterAll(): Unit = {
    onAfterAll()
  }

  "Producer and subscriber" should "work while storage blinks" in {
    val latchStopOnOpen = new CountDownLatch(1)
    val latchStopOnCheckpoint = new CountDownLatch(1)
    val latchFinal = new CountDownLatch(1)
    val subscriberLatch = new CountDownLatch(1)

    val pause = 5000

    new Thread(() => {
      Try({
        val srv = TestStorageServer.getNewClean()
        latchStopOnOpen.await()
        srv
      }).map(srv => TestStorageServer.dispose(srv))

      Thread.sleep(pause)

      Try({
        val srv = TestStorageServer.get()
        latchStopOnCheckpoint.await()
        srv
      }).map(srv => TestStorageServer.dispose(srv))

      Thread.sleep(pause)

      Try({
        val srv = TestStorageServer.get()
        latchFinal.await()
        srv
      }).map(srv => TestStorageServer.dispose(srv))

    }).start()

    val producer = f.getProducer(
      name = "producer",
      partitions = Set(0))

    val subscriber = f.getSubscriber(name = "subscriber",
      partitions = Set(0),
      offset = Oldest,
      useLastOffset = true,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => subscriberLatch.countDown()).start()

    latchStopOnOpen.countDown()
    producer.newTransaction().send("")
    latchStopOnCheckpoint.countDown()
    producer.checkpoint()
    val transactionTTL = f.getProperty(ConfigurationOptions.Producer.Transaction.ttlMs)
    subscriberLatch.await(pause * 2 + transactionTTL.asInstanceOf[Int], TimeUnit.MILLISECONDS) shouldBe true
    subscriber.stop()
    latchFinal.countDown()
  }
}
