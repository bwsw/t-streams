package agents.integration

import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.common.ResettableCountDownLatch
import com.bwsw.tstreams.debug.GlobalHooks
import com.bwsw.tstreams.env.ConfigurationOptions
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{TestStorageServer, TestUtils}

/**
  * Created by Ivan Kudryavtsev on 05.08.16.
  */
class ProducerUpdateTaskTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  val blockCheckpoint1 = new ResettableCountDownLatch(1)
  val blockCheckpoint2 = new ResettableCountDownLatch(1)
  var flag: Int = 0

  System.setProperty("DEBUG", "true")
  GlobalHooks.addHook(GlobalHooks.transactionUpdateTaskBegin, () => {
    flag = 2
    blockCheckpoint1.countDown
  })

  GlobalHooks.addHook(GlobalHooks.transactionUpdateTaskEnd, () => {
    flag = 3
    blockCheckpoint2.countDown
  })

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
  storageClient.createStream("test_stream", 2, 24 * 3600, "")

  val producer = f.getProducer(
    name = "test_producer",
    partitions = Set(0, 1, 2))


  "BasicProducer.checkpoint with delay in update (test latch in update)" should "complete in ordered way" in {
    blockCheckpoint1.setValue(1)
    blockCheckpoint2.setValue(1)
    val t = producer.newTransaction(policy = NewTransactionProducerPolicy.CheckpointIfOpened)
    t.send("data".getBytes())
    blockCheckpoint1.await()
    t.checkpoint()
    flag = 1
    blockCheckpoint2.await()
    flag shouldBe 1
  }

  "BasicProducer.cancel with delay in update (test latch in update)" should "complete in ordered way" in {
    blockCheckpoint1.setValue(1)
    blockCheckpoint2.setValue(1)
    val t = producer.newTransaction(policy = NewTransactionProducerPolicy.CheckpointIfOpened)
    t.send("data")
    blockCheckpoint1.await()
    t.cancel()
    flag = 1
    blockCheckpoint2.await()
    flag shouldBe 1
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(srv)
    producer.stop()
    onAfterAll()
  }
}
