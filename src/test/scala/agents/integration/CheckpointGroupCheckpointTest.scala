package agents.integration

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.group.CheckpointGroup
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.env.ConfigurationOptions
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._


class CheckpointGroupCheckpointTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  System.setProperty("DEBUG", "true")
  System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "DEBUG")

  f.setProperty(ConfigurationOptions.Stream.NAME, "test_stream").
    setProperty(ConfigurationOptions.Stream.PARTITIONS, 3).
    setProperty(ConfigurationOptions.Stream.TTL, 60 * 10).
    setProperty(ConfigurationOptions.Coordination.CONNECTION_TIMEOUT, 7).
    setProperty(ConfigurationOptions.Coordination.TTL, 7).
    setProperty(ConfigurationOptions.Producer.TRANSPORT_TIMEOUT, 5).
    setProperty(ConfigurationOptions.Producer.Transaction.TTL, 6).
    setProperty(ConfigurationOptions.Producer.Transaction.KEEP_ALIVE, 2).
    setProperty(ConfigurationOptions.Consumer.TRANSACTION_PRELOAD, 10).
    setProperty(ConfigurationOptions.Consumer.DATA_PRELOAD, 10)

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
    useLastOffset = true)

  val consumer2 = f.getConsumer[String](
    name = "test_consumer",
    transactionGenerator = LocalGeneratorCreator.getGen(),
    converter = arrayByteToStringConverter,
    partitions = Set(0, 1, 2),
    offset = Oldest,
    useLastOffset = true)

  consumer.start

  "Group commit" should "checkpoint all AgentsGroup state" in {
    val group = new CheckpointGroup()
    group.add(producer)
    group.add(consumer)

    val transaction1 = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    logger.info("Transaction 1 is " + transaction1.getTransactionID.toString)
    transaction1.send("info1")
    transaction1.checkpoint()

    //move consumer offsets
    consumer.getTransaction(0).get

    //open transaction without close
    val transaction2 = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened, 1)
    logger.info("Transaction 2 is " + transaction2.getTransactionID.toString)
    transaction2.send("info2")

    group.checkpoint()


    consumer2.start()
    //assert that the second transaction was closed and consumer offsets was moved
    assert(consumer2.getTransaction(1).get.getAll().head == "info2")
  }

  override def afterAll(): Unit = {
    producer.stop()
    onAfterAll()
  }
}
