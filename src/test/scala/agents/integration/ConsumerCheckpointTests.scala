package agents.integration

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.env.ConfigurationOptions
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{LocalGeneratorCreator, TestUtils}

/**
  * Created by Ivan Kudryavtsev on 09.09.16.
  */
class ConsumerCheckpointTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  f.setProperty(ConfigurationOptions.Stream.name, "test_stream").
    setProperty(ConfigurationOptions.Stream.partitionsCount, 1).
    setProperty(ConfigurationOptions.Stream.ttl, 60 * 10).
    setProperty(ConfigurationOptions.Coordination.connectionTimeoutMs, 7).
    setProperty(ConfigurationOptions.Coordination.sessionTimeoutMs, 7).
    setProperty(ConfigurationOptions.Producer.TRANSPORT_TIMEOUT, 5).
    setProperty(ConfigurationOptions.Producer.Transaction.TTL, 6).
    setProperty(ConfigurationOptions.Producer.Transaction.KEEP_ALIVE, 2).
    setProperty(ConfigurationOptions.Consumer.TRANSACTION_PRELOAD, 10).
    setProperty(ConfigurationOptions.Consumer.DATA_PRELOAD, 10)

  it should "handle checkpoints correctly" in {

    val CONSUMER_NAME = "test_consumer"

    val producer = f.getProducer[String](
      name = "test_producer",
      transactionGenerator = LocalGeneratorCreator.getGen(),
      converter = stringToArrayByteConverter,
      partitions = Set(0))

    val c1 = f.getConsumer[String](
      name = CONSUMER_NAME,
      transactionGenerator = LocalGeneratorCreator.getGen(),
      converter = arrayByteToStringConverter,
      partitions = Set(0),
      offset = Oldest,
      useLastOffset = false)

    val c2 = f.getConsumer[String](
      name = CONSUMER_NAME,
      transactionGenerator = LocalGeneratorCreator.getGen(),
      converter = arrayByteToStringConverter,
      partitions = Set(0),
      offset = Oldest,
      useLastOffset = true)

    val t1 = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened, 0)
    t1.send("data")
    producer.checkpoint()

    val t2 = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened, 0)
    t2.send("data")
    producer.checkpoint()

    c1.start()
    c1.getTransactionById(0, t1.getTransactionID()).isDefined shouldBe true
    c1.getTransactionById(0, t2.getTransactionID()).isDefined shouldBe true

    c1.getTransaction(0).get.getTransactionID() shouldBe t1.getTransactionID()
    c1.checkpoint()
    c1.stop()

    c2.start()
    c2.getTransaction(0).get.getTransactionID() shouldBe t2.getTransactionID()
    c2.checkpoint()
    c2.getTransaction(0).isDefined shouldBe false
    c2.stop()

    producer.stop()
  }

  override def afterAll(): Unit = {
    onAfterAll()
  }
}
