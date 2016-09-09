package agents.integration

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.env.TSF_Dictionary
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{LocalGeneratorCreator, TestUtils}

/**
  * Created by Ivan Kudryavtsev on 09.09.16.
  */
class ConsumerCheckpointTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils  {
  f.setProperty(TSF_Dictionary.Stream.NAME,"test_stream").
    setProperty(TSF_Dictionary.Stream.PARTITIONS,1).
    setProperty(TSF_Dictionary.Stream.TTL, 60 * 10).
    setProperty(TSF_Dictionary.Coordination.CONNECTION_TIMEOUT, 7).
    setProperty(TSF_Dictionary.Coordination.TTL, 7).
    setProperty(TSF_Dictionary.Producer.TRANSPORT_TIMEOUT, 5).
    setProperty(TSF_Dictionary.Producer.Transaction.TTL, 6).
    setProperty(TSF_Dictionary.Producer.Transaction.KEEP_ALIVE, 2).
    setProperty(TSF_Dictionary.Consumer.TRANSACTION_PRELOAD, 10).
    setProperty(TSF_Dictionary.Consumer.DATA_PRELOAD, 10)

  it should "handle checkpoints correctly" in {
    val producer = f.getProducer[String](
      name = "test_producer",
      txnGenerator = LocalGeneratorCreator.getGen(),
      converter = stringToArrayByteConverter,
      partitions = Set(0),
      isLowPriority = false)

    val c1 = f.getConsumer[String](
      name = "test_consumer",
      txnGenerator = LocalGeneratorCreator.getGen(),
      converter = arrayByteToStringConverter,
      partitions = Set(0),
      offset = Oldest,
      isUseLastOffset = false)

    val c2 = f.getConsumer[String](
      name = "test_consumer",
      txnGenerator = LocalGeneratorCreator.getGen(),
      converter = arrayByteToStringConverter,
      partitions = Set(0),
      offset = Oldest,
      isUseLastOffset = true)

    val t1 = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened, 0)
    t1.send("data")
    producer.checkpoint()

    val t2 = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened, 0)
    t2.send("data")
    producer.checkpoint()

    c1.start()
    c1.getTransactionById(0, t1.getTransactionUUID()).isDefined shouldBe true
    c1.getTransactionById(0, t2.getTransactionUUID()).isDefined shouldBe true

    c1.getTransaction(0).get.getTransactionUUID() shouldBe t1.getTransactionUUID()
    c1.checkpoint()
    c1.stop()

    c2.start()
    c2.getTransaction(0).get.getTransactionUUID() shouldBe t2.getTransactionUUID()
    c2.checkpoint()
    c2.getTransaction(0).isDefined shouldBe false
    c2.stop()

    producer.stop()
  }

  override def afterAll(): Unit = {
    onAfterAll()
  }
}
