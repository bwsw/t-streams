package metadata

import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor
import com.bwsw.tstreams.metadata.{TransactionRecord, TransactionDatabase}
import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
import testutils.{LocalGeneratorCreator, TestUtils}

/**
  * Created by Ivan Kudryavtsev on 24.09.16.
  */
class TransactionDatabaseTests extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  val connectedSession = cluster.connect(randomKeyspace)
  val tsdb = new TransactionDatabase(session = connectedSession, stream = "test")

  it should "return none if no transaction" in {
    tsdb.get(0, LocalGeneratorCreator.getTransaction()) shouldBe None
  }

  it should "put transactions into the db" in {
    val transactionID = LocalGeneratorCreator.getTransaction()
    val asynchronousExecutor = new FirstFailLockableTaskExecutor("testExecutor")
    val rec = TransactionRecord(partition = 0, transactionID = transactionID, count = 1, ttl = 1)
    tsdb.put(rec, asynchronousExecutor) (t => {
      tsdb.get(0, transactionID).get.transactionID shouldBe transactionID
    })
  }

  it should "delete transactions from the db" in {
    val transactionID = LocalGeneratorCreator.getTransaction()
    val rec = TransactionRecord(partition = 0, transactionID = transactionID, count = 1, ttl = 1)
    val asynchronousExecutor = new FirstFailLockableTaskExecutor("testExecutor")
    tsdb.put(rec, asynchronousExecutor) (t => {
      tsdb.get(0, transactionID).get.transactionID shouldBe transactionID
      tsdb.del(0, transactionID)
      tsdb.get(0, transactionID) shouldBe None
    })
  }

  it should "handle big range scans without problems" in {
    val TOTAL = 100
    val putCounter = new CountDownLatch(TOTAL)
    val putTransactions = (0 until TOTAL).map(_ => LocalGeneratorCreator.getTransaction())
    val asynchronousExecutor = new FirstFailLockableTaskExecutor("testExecutor")

    putTransactions.foreach(i =>
    tsdb.put(TransactionRecord(partition = 0, transactionID = i, count = 1, ttl = 600), asynchronousExecutor) (transaction => {
      putCounter.countDown()
    }))
    putCounter.await()

    val lastTransaction = putTransactions.last
    val firstTransaction = putTransactions.head
    val getTransactionsForward = tsdb.scanForward(0, firstTransaction, LocalGeneratorCreator.getTransaction()) (t => t.transactionID != lastTransaction)
    getTransactionsForward.size + 1 shouldBe putTransactions.size

    val getTransactionsBackward = tsdb.scanBackward(0, lastTransaction, firstTransaction) (t => t.transactionID != firstTransaction)
    getTransactionsBackward.size + 1 shouldBe putTransactions.size

    getTransactionsForward.tail.map(t => t.transactionID) shouldBe getTransactionsBackward.tail.reverse.map(t => t.transactionID)
    getTransactionsBackward.reverse.map(t => t.transactionID) shouldBe putTransactions.tail
  }

  it should "handle correct offsets inside interval" in {
    val TOTAL = 100
    val putCounter = new CountDownLatch(TOTAL)
    val veryFirst = LocalGeneratorCreator.getTransaction()
    val putTransactions = (0 until TOTAL).map(_ => LocalGeneratorCreator.getTransaction())
    val asynchronousExecutor = new FirstFailLockableTaskExecutor("testExecutor")

    putTransactions.foreach(i =>
      tsdb.put(TransactionRecord(partition = 0, transactionID = i, count = 1, ttl = 600), asynchronousExecutor) (transaction => {
        putCounter.countDown()
      }))
    putCounter.await()

    val lastTransaction = putTransactions.tail.tail.tail.tail.head
    val firstTransaction = putTransactions.tail.tail.head
    val getTransactionsForward = tsdb.scanForward(0, firstTransaction, lastTransaction) (t => true)
    getTransactionsForward.size shouldBe 3
    getTransactionsForward.head.transactionID shouldBe firstTransaction
    getTransactionsForward.last.transactionID shouldBe lastTransaction

    val getTransactionsForwardInverse = tsdb.scanForward(0, lastTransaction, firstTransaction) (t => true)
    getTransactionsForwardInverse.size shouldBe 0

    val getTransactionsForwardVeryFirst = tsdb.scanForward(0, veryFirst, lastTransaction) (t => true)
    getTransactionsForwardVeryFirst.head.transactionID shouldBe putTransactions.head
    getTransactionsForwardVeryFirst.last.transactionID shouldBe lastTransaction

  }

  override def afterAll(): Unit = {
    onAfterAll()
  }
}
