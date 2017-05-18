package com.bwsw.tstreams.agents.integration

import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.agents.consumer.Offset.Oldest
import com.bwsw.tstreams.agents.producer.RPCProducerTransaction
import com.bwsw.tstreams.streams
import com.bwsw.tstreams.testutils._
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}


class ConsumerTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {
  lazy val gen = LocalGeneratorCreator.getGen()

  lazy val srv = TestStorageServer.get()
  lazy val storageClient = f.getStorageClient()

  val TRANSACTION_TTL = 12000

  lazy val consumer = f.getConsumer(
    name = "test_consumer",
    partitions = Set(0, 1, 2),
    offset = Oldest,
    useLastOffset = true)

  var s: streams.Stream = _

  override def beforeAll(): Unit = {
    srv
    createNewStream()
    s = storageClient.loadStream(DEFAULT_STREAM_NAME)
    consumer.start

  }

  "consumer.getTransaction" should "return None if nothing was sent" in {
    val transaction = consumer.getTransaction(0)
    transaction.isEmpty shouldBe true
  }

  "consumer.getTransactionById" should "return sent transaction" in {
    val transactionID = LocalGeneratorCreator.getTransaction()
    val putCounter = new CountDownLatch(1)
    srv.notifyProducerTransactionCompleted(t => t.transactionID == transactionID && t.state == TransactionStates.Checkpointed, putCounter.countDown())
    storageClient.putTransactionSync(new RPCProducerTransaction(s.id, 1, transactionID, TransactionStates.Opened, -1, TRANSACTION_TTL))
    storageClient.putTransactionSync(new RPCProducerTransaction(s.id, 1, transactionID, TransactionStates.Checkpointed, 2, TRANSACTION_TTL))
    putCounter.await()

    val consumedTransaction = consumer.getTransactionById(1, transactionID).get
    consumedTransaction.getPartition shouldBe 1
    consumedTransaction.getTransactionID shouldBe transactionID
    consumedTransaction.getCount shouldBe 2

    val transaction = consumer.getTransaction(1)
    transaction.isDefined shouldEqual true
  }

  "consumer.getLastTransaction" should "return last checkpointed transaction" in {
    val ALL = 100
    val putCounter = new CountDownLatch(1)
    val transactions = for (i <- 0 until ALL) yield LocalGeneratorCreator.getTransaction()
    val transaction = transactions.head
    srv.notifyProducerTransactionCompleted(t => t.transactionID == transactions.last, putCounter.countDown())

    storageClient.putTransactionSync(new RPCProducerTransaction(s.id, 1, transactions.head, TransactionStates.Opened, -1, TRANSACTION_TTL))
    storageClient.putTransactionSync(new RPCProducerTransaction(s.id, 1, transactions.head, TransactionStates.Checkpointed, 1, TRANSACTION_TTL))

    transactions.drop(1) foreach { t =>
      storageClient.putTransactionSync(new RPCProducerTransaction(s.id, 1, t, TransactionStates.Opened, -1, TRANSACTION_TTL))
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
      storageClient.putTransactionSync(new RPCProducerTransaction(s.id, 1, t, TransactionStates.Opened, -1, TRANSACTION_TTL))
      storageClient.putTransactionSync(new RPCProducerTransaction(s.id, 1, t, TransactionStates.Checkpointed, 1, TRANSACTION_TTL))
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
      storageClient.putTransactionSync(new RPCProducerTransaction(s.id, 1, t, TransactionStates.Opened, -1, TRANSACTION_TTL))
      storageClient.putTransactionSync(new RPCProducerTransaction(s.id, 1, t, TransactionStates.Checkpointed, 1, TRANSACTION_TTL))
    }
    storageClient.putTransactionSync(new RPCProducerTransaction(s.id, 1, LocalGeneratorCreator.getTransaction(), TransactionStates.Opened, -1, TRANSACTION_TTL))
    val transactions2 = for (i <- FIRST until LAST) yield LocalGeneratorCreator.getTransaction()

    srv.notifyProducerTransactionCompleted(t => t.transactionID == transactions2.last && t.state == TransactionStates.Checkpointed, putCounter.countDown())

    transactions2 foreach { t =>
      storageClient.putTransactionSync(new RPCProducerTransaction(s.id, 1, t, TransactionStates.Opened, -1, TRANSACTION_TTL))
      storageClient.putTransactionSync(new RPCProducerTransaction(s.id, 1, t, TransactionStates.Checkpointed, 1, TRANSACTION_TTL))
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
    val transactions = for (i <- 0 until ALL) yield LocalGeneratorCreator.getTransaction()
    val firstTransaction = transactions.head
    val lastTransaction = transactions.tail.tail.tail.head
    transactions foreach { t =>
      storageClient.putTransactionSync(new RPCProducerTransaction(s.id, 1, t, TransactionStates.Opened, 1, TRANSACTION_TTL))
    }
    val res = consumer.getTransactionsFromTo(1, lastTransaction, firstTransaction)
    res.size shouldBe 0
  }

  override def afterAll(): Unit = {
    consumer.stop()
    storageClient.shutdown()
    TestStorageServer.dispose(srv)
    onAfterAll()
  }
}