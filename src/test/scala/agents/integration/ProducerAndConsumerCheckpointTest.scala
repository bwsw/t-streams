package agents.integration

import com.bwsw.tstreams.agents.consumer.ConsumerTransaction
import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.common.TimeTracker
import com.bwsw.tstreams.env.ConfigurationOptions
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
  storageClient.createStream("test_stream", 2, 24 * 3600, "")

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
    val dataToSend = (for (i <- 0 until 100) yield randomKeyspace).sorted
    val transactionsAmount = 1000

    (0 until transactionsAmount) foreach { _ =>
      TimeTracker.update_start("newTransaction")
      val transaction = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
      TimeTracker.update_end("newTransaction")
      dataToSend foreach { part =>
        transaction.send(part.getBytes())
      }
      TimeTracker.update_start("checkpoint")
      transaction.checkpoint()
      TimeTracker.update_end("checkpoint")
    }
    val firstPart = transactionsAmount / 3
    val secondPart = transactionsAmount - firstPart

    var checkVal = true

    consumer.start
    (0 until firstPart) foreach { _ =>
      val transaction: ConsumerTransaction = consumer.getTransaction(0).get
      val data = transaction.getAll().map(i => i.toString).sorted
      consumer.checkpoint()
      checkVal &= data == dataToSend
    }

    consumer2.start
    (0 until secondPart) foreach { _ =>
      val transaction: ConsumerTransaction = consumer2.getTransaction(0).get
      val data = transaction.getAll().map(i => i.toString).sorted
      checkVal &= data == dataToSend
    }

    //assert that is nothing to read
    (0 until Integer.parseInt(f.getProperty(ConfigurationOptions.Stream.partitionsCount).toString)) foreach { _ =>
      checkVal &= consumer2.getTransaction(0).isEmpty
    }

    checkVal shouldBe true
  }

  override def afterAll(): Unit = {
    producer.stop()
    onAfterAll()
    TestStorageServer.dispose(srv)
    TimeTracker.dump()
  }
}