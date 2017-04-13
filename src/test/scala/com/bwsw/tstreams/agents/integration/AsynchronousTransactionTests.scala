package com.bwsw.tstreams.agents.integration

import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.debug.GlobalHooks
import com.bwsw.tstreams.env.ConfigurationOptions
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 02.08.16.
  */
class AsynchronousTransactionTests extends FlatSpec with Matchers
  with BeforeAndAfterAll with TestUtils {

  // required for hooks to work
  System.setProperty("DEBUG", "true")

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

  val srv = TestStorageServer.get()
  val storageClient = f.getStorageClient()
  storageClient.createStream("test_stream", 3, 24 * 3600, "")
  storageClient.shutdown()

  val producer = f.getProducer(
    name = "test_producer",
    partitions = Set(0))

  "Fire async checkpoint by producer and wait when complete" should "consumer get transaction from DB" in {
    val l = new CountDownLatch(1)
    GlobalHooks.addHook(GlobalHooks.afterCommitFailure, () => {
      l.countDown()
    })

    val c = f.getConsumer(
      name = "test_subscriber",
      partitions = Set(0),
      offset = Oldest,
      useLastOffset = true)

    val pTransaction = producer.newTransaction(policy = NewTransactionProducerPolicy.ErrorIfOpened)
    val pl = new CountDownLatch(1)
    srv.notifyProducerTransactionCompleted(t => t.transactionID == pTransaction.getTransactionID() && t.state == TransactionStates.Checkpointed, pl.countDown())
    pTransaction.send("test".getBytes())
    pTransaction.checkpoint(isSynchronous = false)
    l.await()
    pl.await()

    c.start()
    val cTransaction = c.getTransaction(0)

    cTransaction.isDefined shouldBe true
    pTransaction.getTransactionID shouldBe cTransaction.get.getTransactionID
    if (cTransaction.isDefined)
      c.checkpoint()

    c.stop()
  }

  "Fire async checkpoint by producer (with exception) and wait when complete" should "consumer not get transaction from DB" in {
    val l = new CountDownLatch(1)
    GlobalHooks.addHook(GlobalHooks.preCommitFailure, () => {
      l.countDown()
      GlobalHooks.clear()
      throw new Exception("expected")
    })

    val c = f.getConsumer(
      name = "test_subscriber",
      partitions = Set(0),
      offset = Oldest,
      useLastOffset = true)

    val pTransaction = producer.newTransaction(policy = NewTransactionProducerPolicy.ErrorIfOpened)

    pTransaction.send("test".getBytes())
    pTransaction.checkpoint(isSynchronous = false)
    l.await()

    val pTransaction2 = producer.newTransaction(policy = NewTransactionProducerPolicy.ErrorIfOpened)
    val pl = new CountDownLatch(1)
    srv.notifyProducerTransactionCompleted(t => t.transactionID == pTransaction2.getTransactionID() && t.state == TransactionStates.Opened, pl.countDown())

    pl.await()
    c.start()
    val cTransaction = c.getTransaction(0)

    cTransaction.isDefined shouldBe false
    c.stop()
  }

  override def afterAll() = {
    producer.stop()
    TestStorageServer.dispose(srv)
    onAfterAll()
  }
}
