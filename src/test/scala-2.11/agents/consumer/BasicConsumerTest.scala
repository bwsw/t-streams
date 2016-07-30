package agents.consumer

import java.util.UUID
import com.bwsw.tstreams.agents.consumer.Offsets.Oldest
import com.bwsw.tstreams.agents.consumer.BasicConsumerTransaction
import com.bwsw.tstreams.agents.producer.ProducerPolicies
import com.bwsw.tstreams.entities.CommitEntity
import com.bwsw.tstreams.env.TSF_Dictionary
import com.datastax.driver.core.utils.UUIDs
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._


class BasicConsumerTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

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
    val txn = consumer.getTransaction
    txn.isEmpty shouldBe true
  }

  "consumer.getTransactionById" should "return sent transaction" in {
    val totalDataInTxn = 10
    val data = (for (i <- 0 until totalDataInTxn) yield randomString).toList.sorted
    val txn = producer.newTransaction(ProducerPolicies.errorIfOpened, 1)
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
    val retrievedTxnOpt: Option[BasicConsumerTransaction[Array[Byte], String]] = consumer.getLastTransaction(partition = 1)
    val retrievedTxn = retrievedTxnOpt.get
    retrievedTxn.getTxnUUID shouldEqual txn
  }

  override def afterAll(): Unit = {
    producer.stop()
    onAfterAll()
  }
}