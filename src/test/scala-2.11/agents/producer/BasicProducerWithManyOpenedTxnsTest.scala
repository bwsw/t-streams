package agents.producer

import com.bwsw.tstreams.agents.consumer.Offsets.Oldest
import com.bwsw.tstreams.agents.producer._
import com.bwsw.tstreams.env.TSF_Dictionary
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._


class BasicProducerWithManyOpenedTxnsTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

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

  val consumer = f.getConsumer[String](
    name = "test_consumer",
    txnGenerator = LocalGeneratorCreator.getGen(),
    converter = arrayByteToStringConverter,
    partitions = List(0,1,2),
    offset = Oldest,
    isUseLastOffset = true)

  "BasicProducer.newTransaction()" should "return BasicProducerTransaction instance" in {
    val data1 = (for (i <- 0 until 10) yield randomString).sorted
    val data2 = (for (i <- 0 until 10) yield randomString).sorted
    val data3 = (for (i <- 0 until 10) yield randomString).sorted
    val txn1: BasicProducerTransaction[String] = producer.newTransaction(ProducerPolicies.errorIfOpened)
    val txn2: BasicProducerTransaction[String] = producer.newTransaction(ProducerPolicies.errorIfOpened)
    val txn3: BasicProducerTransaction[String] = producer.newTransaction(ProducerPolicies.errorIfOpened)
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

  override def afterAll(): Unit = {
    producer.stop()
    onAfterAll()
  }
}
