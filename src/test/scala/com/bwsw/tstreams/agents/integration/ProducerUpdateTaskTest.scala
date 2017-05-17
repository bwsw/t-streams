package com.bwsw.tstreams.agents.integration

import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.producer.NewProducerTransactionPolicy
import com.bwsw.tstreams.common.ResettableCountDownLatch
import com.bwsw.tstreams.env.ConfigurationOptions
import com.bwsw.tstreams.testutils.{TestStorageServer, TestUtils}
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 05.08.16.
  */
class ProducerUpdateTaskTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  lazy val blockCheckpoint1 = new ResettableCountDownLatch(1)
  lazy val blockCheckpoint2 = new ResettableCountDownLatch(1)
  var flag: Int = 0

  val TRANSACTION_TTL_MS = 2000

  lazy val srv = TestStorageServer.get()
  lazy val storageClient = f.getStorageClient()

  lazy val producer = f.getProducer(
    name = "test_producer",
    partitions = Set(0, 1, 2))

  override def beforeAll(): Unit = {
    f.setProperty(ConfigurationOptions.Stream.name, "test_stream").
      setProperty(ConfigurationOptions.Stream.partitionsCount, 3).
      setProperty(ConfigurationOptions.Stream.ttlSec, 60 * 10).
      setProperty(ConfigurationOptions.Coordination.connectionTimeoutMs, 7000).
      setProperty(ConfigurationOptions.Coordination.sessionTimeoutMs, 7000).
      setProperty(ConfigurationOptions.Producer.transportTimeoutMs, 5000).
      setProperty(ConfigurationOptions.Producer.Transaction.ttlMs, TRANSACTION_TTL_MS).
      setProperty(ConfigurationOptions.Producer.Transaction.keepAliveMs, TRANSACTION_TTL_MS / 4).
      setProperty(ConfigurationOptions.Consumer.transactionPreload, 10).
      setProperty(ConfigurationOptions.Consumer.dataPreload, 10)

    srv

    if(storageClient.checkStreamExists("test_stream"))
      storageClient.deleteStream("test_stream")

    storageClient.createStream("test_stream", 3, 24 * 3600, "")
    storageClient.shutdown()
  }

  it should "complete in ordered way with delay" in {

    val l = new CountDownLatch(1)

    val consumer = f.getConsumer(
      name = "test_consumer",
      partitions = Set(0, 1, 2),
      offset = Oldest,
      useLastOffset = true)
    consumer.start()

    val t = producer.newTransaction(policy = NewProducerTransactionPolicy.CheckpointIfOpened, 0)
    val transactionID = t.getTransactionID
    srv.notifyProducerTransactionCompleted(t => t.transactionID == transactionID && t.state == TransactionStates.Checkpointed, l.countDown())
    t.send("data".getBytes())
    Thread.sleep(TRANSACTION_TTL_MS * 3)
    t.checkpoint()
    l.await()

    val consumerTransactionOpt = consumer.getTransactionById(0, t.getTransactionID)
    consumerTransactionOpt.isDefined shouldBe true
    consumerTransactionOpt.get.getTransactionID shouldBe t.getTransactionID
    consumerTransactionOpt.get.getCount shouldBe 1
    consumer.stop()
  }

  override def afterAll(): Unit = {
    producer.stop()
    TestStorageServer.dispose(srv)
    onAfterAll()
  }
}
