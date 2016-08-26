package agents.integration

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.producer._
import com.bwsw.tstreams.env.TSF_Dictionary
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._


class ProducerWithManyOpenedTxnsTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  f.setProperty(TSF_Dictionary.Stream.NAME,"test_stream").
    setProperty(TSF_Dictionary.Stream.PARTITIONS,3).
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
    txnGenerator = LocalGeneratorCreator.getGen(),
    converter = stringToArrayByteConverter,
    partitions = List(0,1,2),
    isLowPriority = false)

  val consumer = f.getConsumer[String](
    name = "test_consumer",
    txnGenerator = LocalGeneratorCreator.getGen(),
    converter = arrayByteToStringConverter,
    partitions = List(0,1,2),
    offset = Oldest,
    isUseLastOffset = true)
  consumer.start

  "BasicProducer.newTransaction()" should "return BasicProducerTransaction instance" in {
    val data1 = (for (i <- 0 until 10) yield randomString).sorted
    val data2 = (for (i <- 0 until 10) yield randomString).sorted
    val data3 = (for (i <- 0 until 10) yield randomString).sorted
    val txn1: Transaction[String] = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    val txn2: Transaction[String] = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    val txn3: Transaction[String] = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    data1.foreach(x => txn1.send(x))
    data2.foreach(x => txn2.send(x))
    data3.foreach(x => txn3.send(x))
    txn3.checkpoint()
    txn2.checkpoint()
    txn1.checkpoint()

    assert(consumer.getTransaction.get.getAll().sorted == data1)
    assert(consumer.getTransaction.get.getAll().sorted == data2)
    assert(consumer.getTransaction.get.getAll().sorted == data3)
    assert(consumer.getTransaction.isEmpty)
  }

  "BasicProducer.newTransaction()" should "return error if try to open more than 3 txns for 3 partitions if ProducerPolicies.errorIfOpened" in {
    val txn1: Transaction[String] = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    val txn2: Transaction[String] = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    val txn3: Transaction[String] = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    val r: Boolean = try {
      val txn4: Transaction[String] = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
      false
    } catch {
      case e: IllegalStateException => true
    }
    r shouldBe true
    producer.checkpoint()
  }

  "BasicProducer.newTransaction()" should "return ok if try to open more than 3 txns for 3 partitions if ProducerPolicies.checkpointIfOpened" in {
    val txn1: Transaction[String] = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    val txn2: Transaction[String] = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    val txn3: Transaction[String] = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    val r: Boolean = try {
      val txn4: Transaction[String] = producer.newTransaction(NewTransactionProducerPolicy.CheckpointIfOpened)
      true
    } catch {
      case e: IllegalStateException => false
    }
    r shouldBe true
    producer.checkpoint()
  }

  "BasicProducer.newTransaction()" should "return ok if try to open more than 3 txns for 3 partitions if ProducerPolicies.cancelIfOpened" in {
    val txn1: Transaction[String] = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    val txn2: Transaction[String] = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    val txn3: Transaction[String] = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    val r: Boolean = try {
      val txn4: Transaction[String] = producer.newTransaction(NewTransactionProducerPolicy.CancelIfOpened)
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
