package database


import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.streams.{StreamService, TransactionDatabase, TransactionRecord}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.{LocalGeneratorCreator, TestStorageServer, TestUtils}
import transactionService.rpc.TransactionStates

/**
  * Created by Ivan Kudryavtsev on 24.09.16.
  */
class TransactionDatabaseTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  val srv = TestStorageServer.get()
  val storageClient = f.getStorageClient()
  val stream = "test"

  StreamService.createStream(storageClient, stream, 1, 24 * 3600, "")

  val tsdb = new TransactionDatabase(storageClient, stream = stream)

  it should "return none if no transaction" in {
    tsdb.get(0, LocalGeneratorCreator.getTransaction()) shouldBe None
  }

  it should "put transactions into the db" in {
    val transactionID = LocalGeneratorCreator.getTransaction()
    val rec = TransactionRecord(partition = 0, transactionID = transactionID, count = 1, ttl = 1, state = TransactionStates.Opened)
    tsdb.put(rec, true)(t => {
      tsdb.get(0, transactionID).get.transactionID shouldBe transactionID
    })
  }

  it should "handle big range scans without problems" in {
    val TOTAL = 1000
    val putCounter = new CountDownLatch(TOTAL)
    val putTransactions = (0 until TOTAL).map(_ => LocalGeneratorCreator.getTransaction())

    putTransactions.foreach(i =>
      tsdb.put(TransactionRecord(partition = 0, transactionID = i, count = 1, ttl = 600, state = TransactionStates.Opened), true)(transaction => {
        putCounter.countDown()
      }))
    putCounter.await()

    val lastTransaction = putTransactions.last
    val firstTransaction = putTransactions.head
    val getTransactionsForward = tsdb.takeWhileForward(0, firstTransaction, LocalGeneratorCreator.getTransaction())(t => t.transactionID != lastTransaction)
    getTransactionsForward.size + 1 shouldBe putTransactions.size

    val getTransactionsBackward = tsdb.takeWhileBackward(0, lastTransaction, firstTransaction)(t => t.transactionID != firstTransaction)
    getTransactionsBackward.size + 1 shouldBe putTransactions.size

    getTransactionsForward.tail.map(t => t.transactionID) shouldBe getTransactionsBackward.tail.reverse.map(t => t.transactionID)
    getTransactionsBackward.reverse.map(t => t.transactionID) shouldBe putTransactions.tail
  }

  it should "handle correct offsets inside interval when forward scan" in {
    val TOTAL = 100
    val putCounter = new CountDownLatch(TOTAL)
    val veryFirst = LocalGeneratorCreator.getTransaction()
    val putTransactions = (0 until TOTAL).map(_ => LocalGeneratorCreator.getTransaction())

    putTransactions.foreach(i =>
      tsdb.put(TransactionRecord(partition = 0, transactionID = i, count = 1, ttl = 600, state = TransactionStates.Opened), true)(transaction => {
        putCounter.countDown()
      }))
    putCounter.await()

    val lastTransaction = putTransactions.tail.tail.tail.tail.head
    val firstTransaction = putTransactions.tail.tail.head
    val getTransactionsForward = tsdb.takeWhileForward(0, firstTransaction, lastTransaction)(t => true)
    getTransactionsForward.size shouldBe 3
    getTransactionsForward.head.transactionID shouldBe firstTransaction
    getTransactionsForward.last.transactionID shouldBe lastTransaction

    val getTransactionsForwardInverse = tsdb.takeWhileForward(0, lastTransaction, firstTransaction)(t => true)
    getTransactionsForwardInverse.size shouldBe 0

    val getTransactionsForwardVeryFirst = tsdb.takeWhileForward(0, veryFirst, lastTransaction)(t => true)
    getTransactionsForwardVeryFirst.head.transactionID shouldBe putTransactions.head
    getTransactionsForwardVeryFirst.last.transactionID shouldBe lastTransaction
  }

  it should "handle correct offset inside interval when search backward" in {
    val TOTAL1 = 50
    val putCounter1 = new CountDownLatch(TOTAL1)
    val putTransactions1 = (0 until TOTAL1).map(_ => LocalGeneratorCreator.getTransaction())

    putTransactions1.foreach(i =>
      tsdb.put(TransactionRecord(partition = 0, transactionID = i, count = 1, ttl = 600, state = TransactionStates.Opened), true)(transaction => {
        putCounter1.countDown()
      }))
    putCounter1.await()

    val TOTAL2 = 50
    val putCounter2 = new CountDownLatch(TOTAL2)
    val putTransactions2 = (0 until TOTAL1).map(_ => LocalGeneratorCreator.getTransaction())

    putTransactions2.foreach(i =>
      tsdb.put(TransactionRecord(partition = 0, transactionID = i, count = 2, ttl = 600, state = TransactionStates.Opened), true)(transaction => {
        putCounter2.countDown()
      }))
    putCounter2.await()

    tsdb.searchBackward(0, putTransactions2.last, putTransactions1.head)(rec => rec.count == 1).get.transactionID shouldBe putTransactions1.last
    tsdb.searchBackward(0, putTransactions2.tail.tail.head, putTransactions2.head)(rec => rec.count == 2).get.transactionID shouldBe putTransactions2.tail.tail.head
    tsdb.searchBackward(0, putTransactions2.last, putTransactions2.head)(rec => rec.count == 3).isEmpty shouldBe true

  }

  it should "handle scan backward correctly" in {
    val TOTAL = 100
    val putCounter = new CountDownLatch(TOTAL)
    val putTransactions = (0 until TOTAL).map(_ => LocalGeneratorCreator.getTransaction())

    putTransactions.foreach(i =>
      tsdb.put(TransactionRecord(partition = 0, transactionID = i, count = 1, ttl = 600, state = TransactionStates.Opened), true)(transaction => {
        putCounter.countDown()
      }))
    putCounter.await()

    tsdb.takeWhileBackward(0, putTransactions.last, putTransactions.head)(rec => true).map(rec => rec.transactionID).reverse shouldBe putTransactions
  }

  override def afterAll(): Unit = {
    TestStorageServer.dispose(srv)
    onAfterAll()
  }
}