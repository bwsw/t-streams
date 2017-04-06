package agents.integration

import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.agents.producer._
import com.bwsw.tstreams.env.ConfigurationOptions
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._


class ProducerUsedByTwoThreadsSimultaneouslyTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {


  // keep it greater than 3
  val ALL_PARTITIONS = 2
  val COUNT = 10000

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

  val srv = TestStorageServer.get()
  val storageClient = f.getStorageClient()
  storageClient.createStream("test_stream", ALL_PARTITIONS, 24 * 3600, "")

  val producer = f.getProducer(
    name = "test_producer",
    partitions = (0 until ALL_PARTITIONS).toSet)

  it should "work correctly if two different threads uses different partitions" in {
    val l = new CountDownLatch(2)

    val t1 = new Thread(() => {
      (0 until COUNT)
        .foreach(i => {
          val t = producer.newTransaction(NewTransactionProducerPolicy.CheckpointAsyncIfOpened, 0)
          t.send("data")
          t.checkpoint(false)
        })
      l.countDown()
    })

    val t2 = new Thread(() => {
      (0 until COUNT)
        .foreach(i => {
          val t = producer.newTransaction(NewTransactionProducerPolicy.CheckpointAsyncIfOpened, 1)
          t.send("data")
          t.checkpoint(false)
        })
      l.countDown()
    })

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    l.await()

  }


  override def afterAll(): Unit = {
    producer.stop()
    TestStorageServer.dispose(srv)
    onAfterAll()
  }
}
