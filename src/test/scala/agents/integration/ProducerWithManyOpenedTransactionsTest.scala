package agents.integration

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.producer._
import com.bwsw.tstreams.env.ConfigurationOptions
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._


class ProducerWithManyOpenedTransactionsTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  f.setProperty(ConfigurationOptions.Stream.name, "test_stream").
    setProperty(ConfigurationOptions.Stream.partitionsCount, 3).
    setProperty(ConfigurationOptions.Stream.ttlSec, 60 * 10).
    setProperty(ConfigurationOptions.Coordination.connectionTimeoutMs, 7).
    setProperty(ConfigurationOptions.Coordination.sessionTimeoutMs, 7).
    setProperty(ConfigurationOptions.Producer.transportTimeoutMs, 5).
    setProperty(ConfigurationOptions.Producer.Transaction.ttlMs, 6).
    setProperty(ConfigurationOptions.Producer.Transaction.keepAliveMs, 2).
    setProperty(ConfigurationOptions.Consumer.transactionPreload, 10).
    setProperty(ConfigurationOptions.Consumer.dataPreload, 10)

  val producer = f.getProducer(
    name = "test_producer",
    partitions = Set(0, 1, 2))

  val consumer = f.getConsumer(
    name = "test_consumer",
    partitions = Set(0, 1, 2),
    offset = Oldest,
    useLastOffset = true)
  consumer.start

  "BasicProducer.newTransaction()" should "return BasicProducerTransaction instance" in {
    val data1 = (for (i <- 0 until 10) yield randomString).sorted
    val data2 = (for (i <- 0 until 10) yield randomString).sorted
    val data3 = (for (i <- 0 until 10) yield randomString).sorted
    val transaction1: ProducerTransaction = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    val transaction2: ProducerTransaction = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    val transaction3: ProducerTransaction = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    data1.foreach(x => transaction1.send(x))
    data2.foreach(x => transaction2.send(x))
    data3.foreach(x => transaction3.send(x))
    transaction3.checkpoint()
    transaction2.checkpoint()
    transaction1.checkpoint()

    assert(consumer.getTransaction(0).get.getAll().map(i => i.toString).sorted == data1)
    assert(consumer.getTransaction(1).get.getAll().map(i => i.toString).sorted == data2)
    assert(consumer.getTransaction(2).get.getAll().map(i => i.toString).sorted == data3)

    (0 to 2).foreach(p => assert(consumer.getTransaction(p).isEmpty))
  }

  "BasicProducer.newTransaction()" should "return error if try to open more than 3 transactions for 3 partitions if ProducerPolicies.errorIfOpened" in {
    producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    val r: Boolean = try {
      producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
      false
    } catch {
      case e: IllegalStateException => true
    }
    r shouldBe true
    producer.checkpoint()
  }

  "BasicProducer.newTransaction()" should "return ok if try to open more than 3 transactions for 3 partitions if ProducerPolicies.checkpointIfOpened" in {
    producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    val r: Boolean = try {
      producer.newTransaction(NewTransactionProducerPolicy.CheckpointIfOpened)
      true
    } catch {
      case e: IllegalStateException => false
    }
    r shouldBe true
    producer.checkpoint()
  }

  "BasicProducer.newTransaction()" should "return ok if try to open more than 3 transactions for 3 partitions if ProducerPolicies.cancelIfOpened" in {
    producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    val r: Boolean = try {
      producer.newTransaction(NewTransactionProducerPolicy.CancelIfOpened)
      true
    } catch {
      case e: IllegalStateException => false
    }
    r shouldBe true
    producer.checkpoint()
  }

  override def afterAll(): Unit = {
    producer.stop()
    onAfterAll()
  }
}
