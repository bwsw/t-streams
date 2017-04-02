package agents.integration.v20

import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.agents.consumer.ConsumerTransaction
import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.common.TimeTracker
import com.bwsw.tstreams.env.ConfigurationOptions
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._


class ProducerAndConsumerCheckpointTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  f.setProperty(ConfigurationOptions.Stream.name, "test_stream").
    setProperty(ConfigurationOptions.Stream.partitionsCount, 3).
    setProperty(ConfigurationOptions.Stream.ttlSec, 60 * 10).
    setProperty(ConfigurationOptions.Coordination.connectionTimeoutMs, 7000).
    setProperty(ConfigurationOptions.Coordination.sessionTimeoutMs, 7000).
    setProperty(ConfigurationOptions.Producer.transportTimeoutMs, 5000).
    setProperty(ConfigurationOptions.Producer.Transaction.ttlMs, 6000).
    setProperty(ConfigurationOptions.Producer.Transaction.keepAliveMs, 2000).
    setProperty(ConfigurationOptions.Consumer.transactionPreload, 10).
    setProperty(ConfigurationOptions.Consumer.dataPreload, 10).
    setProperty(ConfigurationOptions.Producer.Transaction.batchSize, 100)

  val srv = TestStorageServer.get()
  val storageClient = f.getStorageClient()
  storageClient.createStream("test_stream", 3, 24 * 3600, "")

  val producer = f.getProducer(
    name = "test_producer",
    partitions = Set(0))

  val consumer = f.getConsumer(
    name = "test_consumer",
    partitions = Set(0),
    offset = Oldest,
    useLastOffset = true)

  val consumer2 = f.getConsumer(
    name = "test_consumer",
    partitions = Set(0),
    offset = Oldest,
    useLastOffset = true)


  "producer, consumer" should "producer - generate many transactions, consumer - retrieve all of them with reinitialization after some time" in {
    val TRANSACTIONS_COUNT = 1000
    val dataToSend = (for (i <- 0 until 10000) yield randomKeyspace).sorted

    var counter = 0

    val l = new CountDownLatch(1)

    //todo: fixit
    val start = System.currentTimeMillis()

    (0 until TRANSACTIONS_COUNT) foreach { _ =>
      TimeTracker.update_start("newTransaction")
      val transaction = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)

      counter += 1
      if(counter == TRANSACTIONS_COUNT)
        srv.notifyProducerTransactionCompleted(t => t.transactionID == transaction.getTransactionID() && t.state == TransactionStates.Checkpointed, l.countDown())

      TimeTracker.update_end("newTransaction")

      TimeTracker.update_start("Send")
      dataToSend foreach { part => transaction.send(part.getBytes()) }
      TimeTracker.update_end("Send")

      TimeTracker.update_start("checkpoint")
      transaction.checkpoint()
      TimeTracker.update_end("checkpoint")
    }

    val end = System.currentTimeMillis()
    println(end - start)

    val firstPart = TRANSACTIONS_COUNT / 3
    val secondPart = TRANSACTIONS_COUNT - firstPart

    l.await()

    consumer.start
    (0 until firstPart) foreach { _ =>
      val transaction: ConsumerTransaction = consumer.getTransaction(0).get
      transaction.getAll().map(i => new String(i)).sorted shouldBe dataToSend
      consumer.checkpoint()
    }

    //todo: fixit
    Thread.sleep(1000)

    consumer2.start
    (0 until secondPart) foreach { _ =>
      val transaction: ConsumerTransaction = consumer2.getTransaction(0).get
      transaction.getAll().map(i => new String(i)).sorted shouldBe dataToSend
    }

    //assert that is nothing to read
    (0 until Integer.parseInt(f.getProperty(ConfigurationOptions.Stream.partitionsCount).toString)) foreach { _ =>
      consumer2.getTransaction(0).isEmpty shouldBe true
    }
  }

  override def afterAll(): Unit = {
    producer.stop()
    onAfterAll()
    TestStorageServer.dispose(srv)
    TimeTracker.dump()
  }
}