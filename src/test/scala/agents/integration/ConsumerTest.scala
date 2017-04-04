package agents.integration

import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.producer.RPCProducerTransaction
import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor
import com.bwsw.tstreams.env.ConfigurationOptions
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils._


class ConsumerTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  f.setProperty(ConfigurationOptions.Stream.name, "test_stream")
      .setProperty(ConfigurationOptions.Stream.partitionsCount, 3)
      .setProperty(ConfigurationOptions.Stream.ttlSec, 60 * 10)
      .setProperty(ConfigurationOptions.Coordination.connectionTimeoutMs, 7)
      .setProperty(ConfigurationOptions.Coordination.sessionTimeoutMs, 7)
      .setProperty(ConfigurationOptions.Producer.transportTimeoutMs, 5)
      .setProperty(ConfigurationOptions.Producer.Transaction.ttlMs, 6)
      .setProperty(ConfigurationOptions.Producer.Transaction.keepAliveMs, 2)
      .setProperty(ConfigurationOptions.Consumer.transactionPreload, 80)
      .setProperty(ConfigurationOptions.Consumer.dataPreload, 10)

  val gen = LocalGeneratorCreator.getGen()

  val srv = TestStorageServer.get()
  val storageClient = f.getStorageClient()
  storageClient.createStream("test_stream", 2, 24 * 3600, "")


  val consumer = f.getConsumer(
    name = "test_consumer",
    partitions = Set(0, 1, 2),
    offset = Oldest,
    useLastOffset = true)

  consumer.start

  val executor = new FirstFailLockableTaskExecutor("executor")

  "consumer.getTransaction" should "return None if nothing was sent" in {
    val transaction = consumer.getTransaction(0)
    transaction.isEmpty shouldBe true
  }

  "consumer.getTransactionById" should "return sent transaction" in {
    val transactionID = LocalGeneratorCreator.getTransaction()
    val putCounter = new CountDownLatch(1)
    srv.notifyProducerTransactionCompleted(t => t.transactionID == transactionID && t.state == TransactionStates.Checkpointed, putCounter.countDown())
    storageClient.putTransaction(new RPCProducerTransaction("test_stream", 1, transactionID, TransactionStates.Opened, -1, 120), true) { r => true }
    storageClient.putTransaction(new RPCProducerTransaction("test_stream", 1, transactionID, TransactionStates.Checkpointed, 2, 120), true) { r => true }
    putCounter.await()

    val consumedTransaction = consumer.getTransactionById(1, transactionID).get
    consumedTransaction.getPartition shouldBe 1
    consumedTransaction.getTransactionID shouldBe transactionID
    consumedTransaction.getCount() shouldBe 2

    val transaction = consumer.getTransaction(1)
    transaction.isDefined shouldEqual true
  }

  "consumer.getLastTransaction" should "return last checkpointed transaction" in {
    val ALL = 100
    val putCounter = new CountDownLatch(1)
    val transactions = for (i <- 0 until ALL) yield LocalGeneratorCreator.getTransaction()
    val transaction = transactions.head
    srv.notifyProducerTransactionCompleted(t => t.transactionID == transactions.last, putCounter.countDown())

    storageClient.putTransaction(new RPCProducerTransaction("test_stream", 1, transactions.head, TransactionStates.Opened, -1, 120), true) { r => true }
    storageClient.putTransaction(new RPCProducerTransaction("test_stream", 1, transactions.head, TransactionStates.Checkpointed, 1, 120), true) { r => true }

    transactions.drop(1) foreach { t =>
      storageClient.putTransaction(new RPCProducerTransaction("test_stream", 1, t, TransactionStates.Opened, -1, 120), true) { r => true }
    }
    putCounter.await()

    val retrievedTransaction = consumer.getLastTransaction(1).get
    retrievedTransaction.getTransactionID shouldEqual transaction
  }

  "consumer.getTransactionsFromTo" should "return all transactions if no incomplete" in {
    val ALL = 80
    val putCounter = new CountDownLatch(1)
    val transactions = for (i <- 0 until ALL) yield LocalGeneratorCreator.getTransaction()
    val firstTransaction = transactions.head
    val lastTransaction = transactions.last
    srv.notifyProducerTransactionCompleted(t => t.transactionID == lastTransaction && t.state == TransactionStates.Checkpointed, putCounter.countDown())

    transactions foreach { t =>
      storageClient.putTransaction(new RPCProducerTransaction("test_stream", 1, t, TransactionStates.Opened, -1, 120), true) { r => true }
      storageClient.putTransaction(new RPCProducerTransaction("test_stream", 1, t, TransactionStates.Checkpointed, 1, 120), true) { r => true }
    }
    putCounter.await()

    val res = consumer.getTransactionsFromTo(1, firstTransaction, lastTransaction)
    res.size shouldBe transactions.drop(1).size
  }

  "consumer.getTransactionsFromTo" should "return only transactions up to 1st incomplete" in {
    val FIRST = 30
    val LAST = 100
    val putCounter = new CountDownLatch(1)

    val transactions1 = for (i <- 0 until FIRST) yield LocalGeneratorCreator.getTransaction()
    transactions1 foreach { t =>
      storageClient.putTransaction(new RPCProducerTransaction("test_stream", 1, t, TransactionStates.Opened, -1, 120), true) { r => true }
      storageClient.putTransaction(new RPCProducerTransaction("test_stream", 1, t, TransactionStates.Checkpointed, 1, 120), true) { r => true }
    }
    storageClient.putTransaction(new RPCProducerTransaction("test_stream", 1, LocalGeneratorCreator.getTransaction(), TransactionStates.Opened, -1, 120), true) { r => true }
    val transactions2 = for (i <- FIRST until LAST) yield LocalGeneratorCreator.getTransaction()

    srv.notifyProducerTransactionCompleted(t => t.transactionID == transactions2.last && t.state == TransactionStates.Checkpointed, putCounter.countDown())

    transactions2 foreach { t =>
      storageClient.putTransaction(new RPCProducerTransaction("test_stream", 1, t, TransactionStates.Opened, -1, 120), true) { r => true }
      storageClient.putTransaction(new RPCProducerTransaction("test_stream", 1, t, TransactionStates.Checkpointed, 1, 120), true) { r => true }
    }
    putCounter.await()

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
    val ALL = 80
    val putCounter = new CountDownLatch(ALL)
    val transactions = for (i <- 0 until ALL) yield LocalGeneratorCreator.getTransaction()
    val firstTransaction = transactions.head
    val lastTransaction = transactions.tail.tail.tail.head
    transactions foreach { t =>
      storageClient.putTransaction(new RPCProducerTransaction("test_stream", 1, t, TransactionStates.Opened, 1, 120), true) { r => {
        putCounter.countDown()
      }}
    }
    putCounter.await()
    val res = consumer.getTransactionsFromTo(1, lastTransaction, firstTransaction)
    res.size shouldBe 0
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(srv)
    onAfterAll()
  }
}