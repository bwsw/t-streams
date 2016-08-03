package agents.producer

import com.bwsw.tstreams.agents.producer._
import com.bwsw.tstreams.env.TSF_Dictionary
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._


class BasicProducerTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  f.setProperty(TSF_Dictionary.Stream.name,"test_stream").
    setProperty(TSF_Dictionary.Stream.partitions,3).
    setProperty(TSF_Dictionary.Stream.ttl, 60 * 10).
    setProperty(TSF_Dictionary.Coordination.connection_timeout, 7).
    setProperty(TSF_Dictionary.Coordination.ttl, 7).
    setProperty(TSF_Dictionary.Producer.master_timeout, 5).
    setProperty(TSF_Dictionary.Producer.Transaction.ttl, 6).
    setProperty(TSF_Dictionary.Producer.Transaction.keep_alive, 2).
    setProperty(TSF_Dictionary.Consumer.transaction_preload, 10).
    setProperty(TSF_Dictionary.Consumer.data_preload, 10)

  val producer = f.getProducer[String](
    name = "test_producer",
    txnGenerator = LocalGeneratorCreator.getGen(),
    converter = stringToArrayByteConverter,
    partitions = List(0,1,2),
    isLowPriority = false)

  "BasicProducer.newTransaction()" should "return BasicProducerTransaction instance" in {
    val txn: BasicProducerTransaction[String] = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    txn.checkpoint()
    txn.isInstanceOf[BasicProducerTransaction[_]] shouldEqual true
  }

  "BasicProducer.newTransaction(ProducerPolicies.errorIfOpen)" should "throw exception if previous transaction was not closed" in {
    val txn1: BasicProducerTransaction[String] = producer.newTransaction(NewTransactionProducerPolicy.CheckpointIfOpened, 2)
    intercept[IllegalStateException] {
      producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened, 2)
    }
    txn1.checkpoint()
  }

  "BasicProducer.newTransaction(checkpointIfOpen)" should "not throw exception if previous transaction was not closed" in {
    producer.newTransaction(NewTransactionProducerPolicy.CheckpointIfOpened, 2)
    val txn2 = producer.newTransaction(NewTransactionProducerPolicy.CheckpointIfOpened, 2)
    txn2.checkpoint()
  }

  "BasicProducer.getTransaction()" should "return transaction reference if it was created or None" in {
    val txn = producer.newTransaction(NewTransactionProducerPolicy.CheckpointIfOpened, 1)
    val txnRef = producer.getOpenTransactionForPartition(1)
    txn.checkpoint()
    val checkVal = txnRef.get == txn
    checkVal shouldEqual true
  }

  override def afterAll(): Unit = {
    producer.stop()
    onAfterAll()
  }
}
