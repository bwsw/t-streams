package agents.integration

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.producer._
import com.bwsw.tstreams.env.TSF_Dictionary
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._


class ProducerWithManyOpenedTransactionsTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  f.setProperty(TSF_Dictionary.Stream.NAME, "test_stream").
    setProperty(TSF_Dictionary.Stream.PARTITIONS, 3).
    setProperty(TSF_Dictionary.Stream.TTL, 60 * 10).
    setProperty(TSF_Dictionary.Coordination.CONNECTION_TIMEOUT, 7).
    setProperty(TSF_Dictionary.Coordination.TTL, 7).
    setProperty(TSF_Dictionary.Producer.TRANSPORT_TIMEOUT, 5).
    setProperty(TSF_Dictionary.Producer.Transaction.TTL, 6).
    setProperty(TSF_Dictionary.Producer.Transaction.KEEP_ALIVE, 2).
    setProperty(TSF_Dictionary.Consumer.TRANSACTION_PRELOAD, 10).
    setProperty(TSF_Dictionary.Consumer.DATA_PRELOAD, 10)

  val producer = f.getProducer[String](
    name = "test_producer",
    transactionGenerator = LocalGeneratorCreator.getGen(),
    converter = stringToArrayByteConverter,
    partitions = Set(0, 1, 2))

  val consumer = f.getConsumer[String](
    name = "test_consumer",
    transactionGenerator = LocalGeneratorCreator.getGen(),
    converter = arrayByteToStringConverter,
    partitions = Set(0, 1, 2),
    offset = Oldest,
    isUseLastOffset = true)
  consumer.start

  "BasicProducer.newTransaction()" should "return BasicProducerTransaction instance" in {
    val data1 = (for (i <- 0 until 10) yield randomString).sorted
    val data2 = (for (i <- 0 until 10) yield randomString).sorted
    val data3 = (for (i <- 0 until 10) yield randomString).sorted
    val transaction1: ProducerTransaction[String] = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    val transaction2: ProducerTransaction[String] = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    val transaction3: ProducerTransaction[String] = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    data1.foreach(x => transaction1.send(x))
    data2.foreach(x => transaction2.send(x))
    data3.foreach(x => transaction3.send(x))
    transaction3.checkpoint()
    transaction2.checkpoint()
    transaction1.checkpoint()

    assert(consumer.getTransaction(0).get.getAll().sorted == data1)
    assert(consumer.getTransaction(1).get.getAll().sorted == data2)
    assert(consumer.getTransaction(2).get.getAll().sorted == data3)

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
