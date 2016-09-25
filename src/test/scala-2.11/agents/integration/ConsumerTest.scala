package agents.integration


import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy
import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor
import com.bwsw.tstreams.env.TSF_Dictionary
import com.bwsw.tstreams.metadata.{TransactionDatabase, TransactionRecord}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._


class ConsumerTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
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

  val gen = LocalGeneratorCreator.getGen()

  val consumer = f.getConsumer[String](
    name = "test_consumer",
    transactionGenerator = gen,
    converter = arrayByteToStringConverter,
    partitions = Set(0, 1, 2),
    offset = Oldest,
    isUseLastOffset = true)

  val executor = new FirstFailLockableTaskExecutor("executor")
  val tsdb = new TransactionDatabase(cluster.connect(randomKeyspace), "test_stream")

  "consumer.getTransaction" should "return None if nothing was sent" in {
    consumer.start
    val transaction = consumer.getTransaction(0)
    transaction.isEmpty shouldBe true
  }

  "consumer.getTransactionById" should "return sent transaction" in {
    val transactionID = LocalGeneratorCreator.getTransaction()
    tsdb.put(TransactionRecord(1, transactionID, 2, 120), executor) {r => true}
    val consumedTransaction = consumer.getTransactionById(1, transactionID).get
    consumedTransaction.getPartition shouldBe 1
    consumedTransaction.getTransactionID shouldBe transactionID
    consumedTransaction.getCount() shouldBe 2
  }

  "consumer.getTransaction" should "return sent transaction" in {
    val transaction = consumer.getTransaction(1)
    transaction.isDefined shouldEqual true
  }

  "consumer.getLastTransaction" should "return last closed transaction" in {
    val ALL = 100
    val transactions = for (i <- 0 until ALL) yield LocalGeneratorCreator.getTransaction()
    val transaction = transactions.head
    tsdb.put(TransactionRecord(1, transactions.head, 1, 120), executor) {r => true}
    transactions.drop(1) foreach { t =>
      tsdb.put(TransactionRecord(1, t, -1, 120), executor) {r => true}
    }
    val retrievedTransaction = consumer.getLastTransaction(partition = 1).get
    retrievedTransaction.getTransactionID shouldEqual transaction
  }

  "consumer.getTransactionsFromTo" should "return all transactions if no incomplete" in {
    val ALL = 100
    val transactions = for (i <- 0 until ALL) yield LocalGeneratorCreator.getTransaction()
    val firstTransaction = transactions.head
    val lastTransaction = transactions.last
    transactions foreach { t =>
      tsdb.put(TransactionRecord(1, t, 1, 120), executor) {r => true}
    }
    val res = consumer.getTransactionsFromTo(1, firstTransaction, lastTransaction)
    res.size shouldBe transactions.drop(1).size
  }

  "consumer.getTransactionsFromTo" should "return only transactions up to 1st incomplete" in {
    val FIRST = 30
    val LAST = 100
    val transactions1 = for (i <- 0 until FIRST) yield LocalGeneratorCreator.getTransaction()
    transactions1 foreach { t =>
      tsdb.put(TransactionRecord(1, t, 1, 120), executor) {r => true}
    }
    tsdb.put(TransactionRecord(1, LocalGeneratorCreator.getTransaction(), -1, 120), executor) {r => true}
    val transactions2 = for (i <- FIRST until LAST) yield LocalGeneratorCreator.getTransaction()
    transactions2 foreach { t =>
      tsdb.put(TransactionRecord(1, t, 1, 120), executor) {r => true}
    }
    val transactions = transactions1 ++ transactions2
    val firstTransaction = transactions.head
    val lastTransaction = transactions.last

    val res = consumer.getTransactionsFromTo(1, firstTransaction, lastTransaction)
    res.size shouldBe transactions1.drop(1).size
  }

  "consumer.getTransactionsFromTo" should "return none if empty" in {
    val ALL = 100
    val transactions = for (i <- 0 until ALL) yield LocalGeneratorCreator.getTransaction()
    val firstTransaction = transactions.head
    val lastTransaction = transactions.last
    val res = consumer.getTransactionsFromTo(1, firstTransaction, lastTransaction)
    res.size shouldBe 0
  }

  "consumer.getTransactionsFromTo" should "return none if to < from" in {
    val ALL = 100
    val transactions = for (i <- 0 until ALL) yield LocalGeneratorCreator.getTransaction()
    val firstTransaction = transactions.head
    val lastTransaction = transactions.tail.tail.tail.head
    transactions foreach { t =>
      tsdb.put(TransactionRecord(1, t, 1, 120), executor) {r => true}
    }
    val res = consumer.getTransactionsFromTo(1, lastTransaction, firstTransaction)
    res.size shouldBe 0
  }

  override def afterAll(): Unit = {
    onAfterAll()
  }
}