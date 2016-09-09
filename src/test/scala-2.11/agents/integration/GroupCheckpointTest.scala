package agents.integration

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.group.CheckpointGroup
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.env.TSF_Dictionary
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._


class GroupCheckpointTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  System.setProperty("DEBUG", "true")
  System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "DEBUG");

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
    partitions = Set(0, 1, 2),
    isLowPriority = false)

  val consumer = f.getConsumer[String](
    name = "test_consumer",
    txnGenerator = LocalGeneratorCreator.getGen(),
    converter = arrayByteToStringConverter,
    partitions = Set(0, 1, 2),
    offset = Oldest,
    isUseLastOffset = true)

  val consumer2 = f.getConsumer[String](
    name = "test_consumer",
    txnGenerator = LocalGeneratorCreator.getGen(),
    converter = arrayByteToStringConverter,
    partitions = Set(0, 1, 2),
    offset = Oldest,
    isUseLastOffset = true)

  consumer.start

  "Group commit" should "checkpoint all AgentsGroup state" in {
    val group = new CheckpointGroup()
    group.add(producer)
    group.add(consumer)

    val txn1 = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened)
    logger.info("TXN1 is " + txn1.getTransactionUUID.toString)
    txn1.send("info1")
    txn1.checkpoint()

    //move consumer offsets
    consumer.getTransaction(0).get

    //open transaction without close
    val txn2 = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened, 1)
    logger.info("TXN2 is " + txn2.getTransactionUUID.toString)
    txn2.send("info2")

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
