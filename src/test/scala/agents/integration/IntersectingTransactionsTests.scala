package agents.integration

import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.agents.consumer.Offset.Newest
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.env.ConfigurationOptions
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{TestStorageServer, TestUtils}

import scala.collection.mutable.ListBuffer

/**
  * Created by Mikhail Mendelbaum on 02.09.16.
  */
class IntersectingTransactionsTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  f.setProperty(ConfigurationOptions.Stream.name, "test_stream").
    setProperty(ConfigurationOptions.Stream.partitionsCount, 3).
    setProperty(ConfigurationOptions.Stream.ttlSec, 60 * 10).
    setProperty(ConfigurationOptions.Coordination.connectionTimeoutMs, 7000).
    setProperty(ConfigurationOptions.Coordination.sessionTimeoutMs, 7000).
    setProperty(ConfigurationOptions.Producer.transportTimeoutMs, 5000).
    setProperty(ConfigurationOptions.Producer.Transaction.ttlMs, 6000).
    setProperty(ConfigurationOptions.Producer.Transaction.keepAliveMs, 2000).
    setProperty(ConfigurationOptions.Consumer.transactionPreload, 500).
    setProperty(ConfigurationOptions.Consumer.dataPreload, 10)


  val srv = TestStorageServer.get()
  val storageClient = f.getStorageClient()
  storageClient.createStream("test_stream", 3, 24 * 3600, "")
  storageClient.shutdown()

  it should "handle all transactions produced by two different producers, the first ends first started " in {
    val bp = ListBuffer[Long]()
    val bs = ListBuffer[Long]()
    val lp1 = new CountDownLatch(1)
    val lp2 = new CountDownLatch(1)
    val ls = new CountDownLatch(1)

    val producer1 = f.getProducer(
      name = "test_producer1",
      partitions = Set(0))

    val producer2 = f.getProducer(
      name = "test_producer2",
      partitions = Set(0))

    val s = f.getSubscriber(name = "ss+2",
      partitions = Set(0),
      offset = Newest,
      useLastOffset = true,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => this.synchronized {
        bs.append(transaction.getTransactionID())
        if (bs.size == 2) {
          ls.countDown()
        }
      })

    val t1 = new Thread(() => {
      val t = producer1.newTransaction(policy = NewTransactionProducerPolicy.CheckpointIfOpened)
      bp.append(t.getTransactionID())
      lp2.countDown()
      lp1.await()
      t.send("test".getBytes())
      t.checkpoint()
    })

    val t2 = new Thread(() => {
      lp2.await()
      val t = producer2.newTransaction(policy = NewTransactionProducerPolicy.CheckpointIfOpened)
      bp.append(t.getTransactionID())
      t.send("test".getBytes())
      t.checkpoint()
      lp1.countDown()
    })

    s.start()

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    ls.await()

    producer1.stop()
    producer2.stop()
    s.stop()

    bp.head shouldBe bs.head
    bp.tail.head shouldBe bs.tail.head
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(srv)
    onAfterAll()
  }

}
