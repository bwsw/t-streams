package agents.subscriber

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreams.agents.consumer.subscriber.{Callback, ProcessingEngine, QueueBuilder}
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import org.scalatest.{FlatSpec, Matchers}
import testutils.LocalGeneratorCreator

import scala.collection.mutable.ListBuffer

class ProcessingEngineOperatorTestImpl extends TransactionOperator[String] {

  val TOTAL = 10
  val transactions = new ListBuffer[ConsumerTransaction[String]]()
  for (i <- 0 until TOTAL)
    transactions += new ConsumerTransaction[String](0, LocalGeneratorCreator.getTransaction(), 1, -1)

  var lastTransaction: Option[ConsumerTransaction[String]] = None

  override def getLastTransaction(partition: Int): Option[ConsumerTransaction[String]] = lastTransaction

  override def getTransactionById(partition: Int, id: Long): Option[ConsumerTransaction[String]] = None

  override def setStreamPartitionOffset(partition: Int, id: Long): Unit = {}

  override def loadTransactionFromDB(partition: Int, transactionID: Long): Option[ConsumerTransaction[String]] = None

  override def checkpoint(): Unit = {}

  override def getPartitions(): Set[Int] = Set[Int](0)

  override def getCurrentOffset(partition: Int): Long = LocalGeneratorCreator.getTransaction()

  override def buildTransactionObject(partition: Int, id: Long, count: Int): Option[ConsumerTransaction[String]] = None

  override def getTransactionsFromTo(partition: Int, from: Long, to: Long): ListBuffer[ConsumerTransaction[String]] = transactions
}

/**
  * Created by Ivan Kudryavtsev on 22.08.16.
  */
class ProcessingEngineTests extends FlatSpec with Matchers {

  val cb = new Callback[String] {
    override def onTransaction(consumer: TransactionOperator[String], transaction: ConsumerTransaction[String]): Unit = {}
  }
  val qb = new QueueBuilder.InMemory()

  val gen = LocalGeneratorCreator.getGen()


  "constructor" should "create Processing engine" in {
    val pe = new ProcessingEngine[String](new ProcessingEngineOperatorTestImpl(), Set[Int](0), qb, cb)
  }

  "enqueueLastTransactionFromDB" should "not Enqueue last transaction state to Queue if it's not defined" in {
    val pe = new ProcessingEngine[String](new ProcessingEngineOperatorTestImpl(), Set[Int](0), qb, cb)
    pe.enqueueLastTransactionFromDB(0)
    val elt = pe.getQueue().get(500, TimeUnit.MILLISECONDS)
    elt shouldBe null
  }

  "enqueueLastTransactionFromDB" should "enqueue last transaction state to Queue if it's newer than we have in our database" in {
    val c = new ProcessingEngineOperatorTestImpl()
    val pe = new ProcessingEngine[String](c, Set[Int](0), qb, cb)
    c.lastTransaction = Option[ConsumerTransaction[String]](new ConsumerTransaction(0, LocalGeneratorCreator.getTransaction(), 1, -1))
    pe.enqueueLastTransactionFromDB(0)
    val elt = pe.getQueue().get(500, TimeUnit.MILLISECONDS)
    elt.head.transactionID shouldBe c.lastTransaction.get.getTransactionID()
  }

  "enqueueLastTransactionFromDB" should "not enqueue last transaction state to Queue if it's older than we have in our database" in {
    val c = new ProcessingEngineOperatorTestImpl()
    c.lastTransaction = Option[ConsumerTransaction[String]](new ConsumerTransaction(0, LocalGeneratorCreator.getTransaction(), 1, -1))
    val pe = new ProcessingEngine[String](c, Set[Int](0), qb, cb)
    pe.enqueueLastTransactionFromDB(0)
    val elt = pe.getQueue().get(500, TimeUnit.MILLISECONDS)
    elt shouldBe null
  }

  "handleQueue" should "do nothing if there is nothing in queue" in {
    val c = new ProcessingEngineOperatorTestImpl()
    val pe = new ProcessingEngine[String](c, Set[Int](0), qb, cb)
    val act1 = pe.getLastPartitionActivity(0)
    pe.handleQueue(500)
    val act2 = pe.getLastPartitionActivity(0)
    act1 shouldBe act2
  }

  "handleQueue" should "do fast/full load if there is seq in queue" in {
    val c = new ProcessingEngineOperatorTestImpl()
    val pe = new ProcessingEngine[String](c, Set[Int](0), qb, cb)
    c.lastTransaction = Option[ConsumerTransaction[String]](new ConsumerTransaction(0, LocalGeneratorCreator.getTransaction(), 1, -1))
    pe.enqueueLastTransactionFromDB(0)
    val act1 = pe.getLastPartitionActivity(0)
    pe.handleQueue(500)
    val act2 = pe.getLastPartitionActivity(0)
    act2 - act1 > 0 shouldBe true
  }

  "handleQueue" should "enqueue last transaction to queue if polling interval have been expired" in {
    val c = new ProcessingEngineOperatorTestImpl()
    val pe = new ProcessingEngine[String](c, Set[Int](0), qb, cb)
    c.lastTransaction = Option[ConsumerTransaction[String]](new ConsumerTransaction(0, LocalGeneratorCreator.getTransaction(), 1, -1))
    val act1 = pe.getLastPartitionActivity(0)
    val POLLING_DELAY = 5
    Thread.sleep(1)
    pe.handleQueue(POLLING_DELAY)
    pe.enqueueLastTransactionFromDB(0)
    val elt = pe.getQueue().get(500, TimeUnit.MILLISECONDS)
    elt.head.transactionID shouldBe c.lastTransaction.get.getTransactionID()
  }

}
