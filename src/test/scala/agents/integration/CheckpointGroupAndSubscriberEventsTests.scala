package agents.integration

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.Offset.Newest
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.agents.group.CheckpointGroup
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.env.ConfigurationOptions
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{TestStorageServer, TestUtils}

/**
  * Created by ivan on 13.09.16.
  */
class CheckpointGroupAndSubscriberEventsTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

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

  val producer = f.getProducer(
    name = "test_producer",
    partitions = Set(0))

  "Group commit" should "checkpoint all AgentsGroup state" in {
    val l = new CountDownLatch(1)
    var transactionsCounter: Int = 0

    val group = new CheckpointGroup()

    group.add(producer)

    val subscriber = f.getSubscriber(name = "ss+2",
      partitions = Set(0),
      offset = Newest,
      useLastOffset = true,
      callback = (consumer: TransactionOperator, transaction: ConsumerTransaction) => this.synchronized {
        transactionsCounter += 1
        if (transactionsCounter == 2) {
          l.countDown()
        }
      })
    subscriber.start()
    val txn1 = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened, 0)
    txn1.send("test".getBytes())
    group.checkpoint()
    val txn2 = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened, 0)
    txn2.send("test".getBytes())
    group.checkpoint()
    l.await(5, TimeUnit.SECONDS) shouldBe true
    transactionsCounter shouldBe 2
    subscriber.stop()
  }

  override def afterAll(): Unit = {
    producer.stop()
    TestStorageServer.dispose(srv)
    onAfterAll()
  }
}