package com.bwsw.tstreams.agents.subscriber

import com.bwsw.tstreams.agents.consumer.subscriber.{Callback, ProcessingEngine, QueueBuilder}
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.testutils.LocalGeneratorCreator
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer

class ProcessingEngineOperatorTestImpl extends TransactionOperator {

  val TOTAL = 10
  val transactions = new ListBuffer[ConsumerTransaction]()
  for (i <- 0 until TOTAL)
    transactions += new ConsumerTransaction(0, LocalGeneratorCreator.getTransaction(), 1, -1)

  var lastTransaction: Option[ConsumerTransaction] = None

  override def getLastTransaction(partition: Int): Option[ConsumerTransaction] = lastTransaction

  override def getTransactionById(partition: Int, id: Long): Option[ConsumerTransaction] = None

  override def setStreamPartitionOffset(partition: Int, id: Long): Unit = {}

  override def loadTransactionFromDB(partition: Int, transactionID: Long): Option[ConsumerTransaction] = None

  override def checkpoint(): Unit = {}

  override def getPartitions(): Set[Int] = Set[Int](0)

  override def getCurrentOffset(partition: Int): Long = LocalGeneratorCreator.getTransaction()

  override def buildTransactionObject(partition: Int, id: Long, count: Int): Option[ConsumerTransaction] = None

  override def getTransactionsFromTo(partition: Int, from: Long, to: Long): ListBuffer[ConsumerTransaction] = transactions

  override def getProposedTransactionId(): Long = LocalGeneratorCreator.getTransaction()
}

/**
  * Created by Ivan Kudryavtsev on 22.08.16.
  */
class ProcessingEngineTests extends FlatSpec with Matchers {

  val cb = new Callback {
    override def onTransaction(consumer: TransactionOperator, transaction: ConsumerTransaction): Unit = {}
  }
  val qb = new QueueBuilder.InMemory()

  val gen = LocalGeneratorCreator.getGen()


  "constructor" should "create Processing engine" in {
    val pe = new ProcessingEngine(new ProcessingEngineOperatorTestImpl(), Set[Int](0), qb, cb, 100)
  }

  "handleQueue" should "do nothing if there is nothing in queue" in {
    val c = new ProcessingEngineOperatorTestImpl()
    val pe = new ProcessingEngine(c, Set[Int](0), qb, cb, 100)
    val act1 = pe.getLastPartitionActivity(0)
    pe.handleQueue(10)
    val act2 = pe.getLastPartitionActivity(0)
    act1 shouldBe act2
  }

  "handleQueue" should "do fast/full load if there is seq in queue" in {
    val c = new ProcessingEngineOperatorTestImpl()
    val pe = new ProcessingEngine(c, Set[Int](0), qb, cb, 100)
    c.lastTransaction = Option[ConsumerTransaction](new ConsumerTransaction(0, LocalGeneratorCreator.getTransaction(), 1, -1))
    pe.enqueueLastPossibleTransaction(0)
    val act1 = pe.getLastPartitionActivity(0)
    Thread.sleep(10)
    pe.handleQueue(10)
    val act2 = pe.getLastPartitionActivity(0)
    act2 - act1 > 0 shouldBe true
  }


}
