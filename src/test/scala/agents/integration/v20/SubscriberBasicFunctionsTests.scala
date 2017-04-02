package agents.integration.v20

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.env.ConfigurationOptions
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{TestStorageServer, TestUtils}

import scala.collection.mutable

/**
  * Created by Ivan Kudryavtsev on 24.08.16.
  */
class SubscriberBasicFunctionsTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

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

  val producer = f.getProducer(
    name = "test_producer",
    partitions = Set(0, 1, 2))

  it should "start and stop with default options" in {
    val s = f.getSubscriber(name = "sv2",
      partitions = Set(0, 1, 2),
      offset = Oldest,
      useLastOffset = true,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => {})
    s.start()
    s.stop()
  }

  it should "allow start and stop several times" in {
    val s = f.getSubscriber(name = "sv2",
      offset = Oldest, partitions = Set(0, 1, 2),
      useLastOffset = true,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => {})
    s.start()
    s.stop()
    s.start()
    s.stop()
  }

  it should "not allow double start" in {
    val s = f.getSubscriber(name = "sv2",
      partitions = Set(0, 1, 2),
      offset = Oldest,
      useLastOffset = true,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => {})
    s.start()
    var flag = false
    flag = try {
      s.start()
      false
    } catch {
      case e: IllegalStateException =>
        true
    }
    flag shouldBe true
    s.stop()
  }

  it should "not allow double stop" in {
    val s = f.getSubscriber(name = "sv2",
      partitions = Set(0, 1, 2),
      offset = Oldest,
      useLastOffset = true,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => {})
    s.start()
    s.stop()
    var flag = false
    flag = try {
      s.stop()
      false
    } catch {
      case e: IllegalStateException =>
        true
    }
    flag shouldBe true
  }

  it should "allow to be created with in memory queues" in {
    val f1 = f.copy()
    val s = f1.getSubscriber(name = "sv2_inram",
      partitions = Set(0, 1, 2),
      offset = Oldest,
      useLastOffset = true,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => {})
    s.start()
    s.stop()
  }

  it should "receive all transactions producer by producer previously" in {
    val l = new CountDownLatch(1)
    val pl = new CountDownLatch(1)
    var i: Int = 0
    val TOTAL = 1000
    var id: Long = 0
    val ps = mutable.ListBuffer[Long]()
    for (it <- 0 until TOTAL) {
      val transaction = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
      transaction.send("test")
      transaction.checkpoint()
      id = transaction.getTransactionID()
      ps.append(id)
    }
    srv.notifyProducerTransactionCompleted(t => t.transactionID == id && t.state == TransactionStates.Checkpointed, pl.countDown())
    producer.stop()
    pl.await()

    val ss = mutable.ListBuffer[Long]()

    val s = f.getSubscriber(name = "sv2",
      partitions = Set(0, 1, 2),
      offset = Oldest,
      useLastOffset = true,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => this.synchronized {
        ss.append(transaction.getTransactionID())
        i += 1
        if (i == TOTAL)
          l.countDown()
      })
    s.start()
    l.await(60, TimeUnit.SECONDS)
    s.stop()
    i shouldBe TOTAL
    ps.sorted shouldBe ss.sorted
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(srv)
    onAfterAll()
  }
}
