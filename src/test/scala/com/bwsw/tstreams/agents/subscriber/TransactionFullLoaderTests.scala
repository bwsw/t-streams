package com.bwsw.tstreams.agents.subscriber

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.subscriber.TransactionFullLoader
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor
import com.bwsw.tstreams.testutils.IncreasingGenerator
import com.bwsw.tstreamstransactionserver.protocol.TransactionState
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class FullLoaderOperatorTestImpl extends TransactionOperator {

  val TOTAL = 10
  val transactions = new ListBuffer[ConsumerTransaction]()
  for (i <- 0 until TOTAL)
    transactions += new ConsumerTransaction(0, IncreasingGenerator.get, 1, TransactionStates.Checkpointed, -1)

  override def getLastTransaction(partition: Int): Option[ConsumerTransaction] = None

  override def getTransactionById(partition: Int, id: Long): Option[ConsumerTransaction] = None

  override def setStreamPartitionOffset(partition: Int, id: Long): Unit = {}

  override def loadTransactionFromDB(partition: Int, transaction: Long): Option[ConsumerTransaction] = None

  override def getTransactionsFromTo(partition: Int, from: Long, to: Long): ListBuffer[ConsumerTransaction] =
    transactions

  override def checkpoint(): Unit = {}

  override def getPartitions(): Set[Int] = Set[Int](0)

  override def getCurrentOffset(partition: Int) = IncreasingGenerator.get

  override def buildTransactionObject(partition: Int, id: Long, state: TransactionStates, count: Int): Option[ConsumerTransaction] = Some(new ConsumerTransaction(0, IncreasingGenerator.get, 1, TransactionStates.Checkpointed, -1))

  override def getProposedTransactionId(): Long = IncreasingGenerator.get
}

trait FullLoaderTestContainer {
  val lastTransactionsMap = mutable.Map[Int, TransactionState]()
  val fullLoader = new TransactionFullLoader(partitions(), lastTransactionsMap)

  def partitions() = Set(0)

  def test()
}


/**
  * Created by Ivan Kudryavtsev on 22.08.16.
  */
class TransactionFullLoaderTests extends FlatSpec with Matchers {

  it should "load if next state is after prev state by id" in {
    val tc = new FullLoaderTestContainer {
      val partition = 0
      val masterID = 0
      val orderID = 0
      lastTransactionsMap(0) = TransactionState(IncreasingGenerator.get, partition, masterID, orderID, 1, TransactionState.Status.Checkpointed, -1)
      val nextTransactionState = List(TransactionState(IncreasingGenerator.get + 1, partition, masterID, orderID + 1, 1, TransactionState.Status.Checkpointed, -1))

      override def test(): Unit = {
        fullLoader.checkIfTransactionLoadingIsPossible(nextTransactionState) shouldBe true
      }
    }

    tc.test()
  }

  it should "not load if next state is before prev state by id" in {
    val tc = new FullLoaderTestContainer {
      val partition = 0
      val masterID = 0
      val orderID = 0
      val nextTransactionState = List(TransactionState(IncreasingGenerator.get, partition, masterID, orderID + 1, 1, TransactionState.Status.Checkpointed, -1))
      lastTransactionsMap(0) = TransactionState(IncreasingGenerator.get + 1, partition, masterID, orderID, 1, TransactionState.Status.Checkpointed, -1)

      override def test(): Unit = {
        fullLoader.checkIfTransactionLoadingIsPossible(nextTransactionState) shouldBe false
      }
    }

    tc.test()
  }

  it should "load all transactions from DB" in {
    val tc = new FullLoaderTestContainer {
      val partition = 0
      val masterID = 0
      val orderID = 0
      lastTransactionsMap(0) = TransactionState(IncreasingGenerator.get, partition, masterID, orderID, 1, TransactionState.Status.Checkpointed, -1)
      val fullLoader2 = new TransactionFullLoader(partitions(), lastTransactionsMap)
      val consumerOuter = new FullLoaderOperatorTestImpl()
      val nextTransactionState = List(TransactionState(consumerOuter.transactions.last.getTransactionID, partition, masterID, orderID + 1, 1, TransactionState.Status.Checkpointed, -1))

      override def test(): Unit = {
        var ctr: Int = 0
        val l = new CountDownLatch(1)
        fullLoader2.load(nextTransactionState,
          consumerOuter,
          new FirstFailLockableTaskExecutor("lf"),
          (consumer: TransactionOperator, transaction: ConsumerTransaction) => {
            ctr += 1
            if (ctr == consumerOuter.TOTAL)
              l.countDown()
          })
        l.await(1, TimeUnit.SECONDS)
        ctr shouldBe consumerOuter.TOTAL
      }
    }

    tc.test()
  }

  it should "load none transactions from DB" in {
    val tc = new FullLoaderTestContainer {
      val partition = 0
      val masterID = 0
      val orderID = 0
      lastTransactionsMap(0) = TransactionState(IncreasingGenerator.get, partition, masterID, orderID, 1, TransactionState.Status.Checkpointed, -1)
      val fullLoader2 = new TransactionFullLoader(partitions(), lastTransactionsMap)
      val consumerOuter = new FullLoaderOperatorTestImpl()
      val nextTransactionState = List(TransactionState(consumerOuter.transactions.last.getTransactionID, partition, masterID, orderID + 1, 1, TransactionState.Status.Checkpointed, -1))

      override def test(): Unit = {
        var ctr: Int = 0
        val l = new CountDownLatch(1)
        fullLoader2.load(nextTransactionState,
          consumerOuter,
          new FirstFailLockableTaskExecutor("lf"),
          (consumer: TransactionOperator, transaction: ConsumerTransaction) => {
            ctr += 1
            if (ctr == consumerOuter.TOTAL)
              l.countDown()
          })
        l.await(1, TimeUnit.SECONDS)
        ctr shouldBe consumerOuter.TOTAL
        lastTransactionsMap(0).transactionID shouldBe consumerOuter.transactions.last.getTransactionID
      }
    }

    tc.test()
  }
}
