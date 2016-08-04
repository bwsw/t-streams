package agents.consumer

import java.util.UUID
import com.bwsw.tstreams.agents.consumer.Offsets.Oldest
import com.bwsw.tstreams.agents.consumer.ConsumerTransaction
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.entities.CommitEntity
import com.bwsw.tstreams.env.TSF_Dictionary
import com.datastax.driver.core.utils.UUIDs
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._


class ConsumerTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  f.setProperty(TSF_Dictionary.Stream.NAME,"test_stream").
    setProperty(TSF_Dictionary.Stream.PARTITIONS,3).
    setProperty(TSF_Dictionary.Stream.TTL, 60 * 10).
    setProperty(TSF_Dictionary.Coordination.CONNECTION_TIMEOUT, 7).
    setProperty(TSF_Dictionary.Coordination.TTL, 7).
    setProperty(TSF_Dictionary.Producer.MASTER_TIMEOUT, 5).
    setProperty(TSF_Dictionary.Producer.Transaction.TTL, 6).
    setProperty(TSF_Dictionary.Producer.Transaction.KEEP_ALIVE, 2).
    setProperty(TSF_Dictionary.Consumer.TRANSACTION_PRELOAD, 10).
    setProperty(TSF_Dictionary.Consumer.DATA_PRELOAD, 10)

  val consumer = f.getConsumer[String](
    name = "test_consumer",
    txnGenerator = LocalGeneratorCreator.getGen(),
    converter = arrayByteToStringConverter,
    partitions = List(0,1,2),
    offset = Oldest,
    isUseLastOffset = true)

  val producer = f.getProducer[String](
    name = "test_producer",
    txnGenerator = LocalGeneratorCreator.getGen(),
    converter = stringToArrayByteConverter,
    partitions = List(0,1,2),
    isLowPriority = false)

  "consumer.getTransaction" should "return None if nothing was sent" in {
    consumer.start
    val txn = consumer.getTransaction
    txn.isEmpty shouldBe true
  }

  "consumer.getTransactionById" should "return sent transaction" in {
    val totalDataInTxn = 10
    val data = (for (i <- 0 until totalDataInTxn) yield randomString).toList.sorted
    val txn = producer.newTransaction(NewTransactionProducerPolicy.ErrorIfOpened, 1)
    val txnUuid = txn.getTxnUUID
    data.foreach(x => txn.send(x))
    txn.checkpoint()
    var checkVal = true

    val consumedTxn = consumer.getTransactionById(1, txnUuid).get
    checkVal = consumedTxn.getPartition == txn.getPartition
    checkVal = consumedTxn.getTxnUUID == txnUuid
    checkVal = consumedTxn.getAll().sorted == data

    checkVal shouldEqual true
  }

  "consumer.getTransaction" should "return sent transaction" in {
    val txn = consumer.getTransaction
    txn.isDefined shouldEqual true
  }

  "consumer.getLastTransaction" should "return last closed transaction" in {
    val commitEntity = new CommitEntity("commit_log", cluster.connect(randomKeyspace))
    val txns = for (i <- 0 until 500) yield UUIDs.timeBased()
    val txn: UUID = txns.head
    commitEntity.commit("test_stream", 1, txns.head, 1, 120)
    txns.drop(1) foreach { x =>
      commitEntity.commit("test_stream", 1, x, -1, 120)
    }
    val retrievedTxnOpt: Option[ConsumerTransaction[String]] = consumer.getLastTransaction(partition = 1)
    val retrievedTxn = retrievedTxnOpt.get
    retrievedTxn.getTxnUUID shouldEqual txn
  }

  override def afterAll(): Unit = {
    producer.stop()
    onAfterAll()
  }
}