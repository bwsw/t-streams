package agents.integration

/**
  * Created by mendelbaum_ma on 08.09.16.
  */

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.Offset.Newest
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.env.ConfigurationOptions
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{TestStorageServer, TestUtils}

import scala.collection.mutable.ListBuffer


class SubscriberWithTwoProducersFirstCancelSecondCheckpointTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

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

  it should "Integration MixIn checkpoint and cancel must be correctly processed on Subscriber " in {

    val bp1 = ListBuffer[Long]()
    val bp2 = ListBuffer[Long]()
    val bs = ListBuffer[Long]()

    val lp2 = new CountDownLatch(1)
    val ls = new CountDownLatch(1)

    val subscriber = f.getSubscriber(name = "ss+2",
      partitions = Set(0),
      offset = Newest,
      useLastOffset = true,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => this.synchronized {
        bs.append(transaction.getTransactionID())
        ls.countDown()
      })

    subscriber.start()

    val producer1 = f.getProducer(
      name = "test_producer1",
      partitions = Set(0))

    val producer2 = f.getProducer(
      name = "test_producer2",
      partitions = Set(0))



    val t1 = new Thread(() => {
      val transaction = producer1.newTransaction(policy = NewTransactionProducerPolicy.CheckpointIfOpened)
      lp2.countDown()
      bp1.append(transaction.getTransactionID())
      transaction.send("test")
      transaction.cancel()
    })

    val t2 = new Thread(() => {
      lp2.await()
      val transaction = producer2.newTransaction(policy = NewTransactionProducerPolicy.CheckpointIfOpened)
      bp2.append(transaction.getTransactionID())
      transaction.send("test")
      transaction.checkpoint()
    })

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    ls.await(10, TimeUnit.SECONDS)

    producer1.stop()
    producer2.stop()

    subscriber.stop()

    bs.size shouldBe 1 // Adopted by only one and it is from second
    bp2.head shouldBe bs.head
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(srv)
    onAfterAll()
  }
}