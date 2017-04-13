package com.bwsw.tstreams.agents.subscriber

import java.util.concurrent.CountDownLatch

import com.bwsw.tstreams.agents.consumer.subscriber.{TransactionFastLoader, TransactionState}
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor
import com.bwsw.tstreams.coordination.messages.state.TransactionStatus
import com.bwsw.tstreams.testutils.LocalGeneratorCreator
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

trait FastLoaderTestContainer {
  val lastTransactionsMap = mutable.Map[Int, TransactionState]()
  val fastLoader = new TransactionFastLoader(partitions(), lastTransactionsMap)

  def partitions() = Set(0)

  def test()
}

class FastLoaderOperatorTestImpl extends TransactionOperator {
  val TOTAL = 10
  val transactions = new ListBuffer[ConsumerTransaction]()

  for (i <- 0 until TOTAL)
    transactions += new ConsumerTransaction(0, LocalGeneratorCreator.getTransaction(), 1, -1)

  override def getLastTransaction(partition: Int): Option[ConsumerTransaction] = None

  override def getTransactionById(partition: Int, id: Long): Option[ConsumerTransaction] = None

  override def setStreamPartitionOffset(partition: Int, id: Long): Unit = {}

  override def loadTransactionFromDB(partition: Int, transaction: Long): Option[ConsumerTransaction] = None

  override def getTransactionsFromTo(partition: Int, from: Long, to: Long): ListBuffer[ConsumerTransaction] =
    transactions

  override def checkpoint(): Unit = {}

  override def getPartitions(): Set[Int] = Set[Int](0)

  override def getCurrentOffset(partition: Int) = LocalGeneratorCreator.getTransaction()

  override def buildTransactionObject(partition: Int, id: Long, count: Int): Option[ConsumerTransaction] = Some(new ConsumerTransaction(0, LocalGeneratorCreator.getTransaction(), 1, -1))

  override def getProposedTransactionId(): Long = LocalGeneratorCreator.getTransaction()
}

/**
  * Created by Ivan Kudryavtsev on 21.08.16.
  */
class TransactionFastLoaderTests extends FlatSpec with Matchers {

  val gen = LocalGeneratorCreator.getGen()

  it should "load fast if next state is after prev state from the same master" in {
    val tc = new FastLoaderTestContainer {
      val partition = 0
      val masterID = 0
      val orderID = 0
      lastTransactionsMap(0) = TransactionState(LocalGeneratorCreator.getTransaction(), partition, masterID, orderID, 1, TransactionStatus.checkpointed, -1)
      val nextTransactionState = List(TransactionState(LocalGeneratorCreator.getTransaction(), partition, masterID, orderID + 1, 1, TransactionStatus.checkpointed, -1))

      override def test(): Unit = {
        fastLoader.checkIfTransactionLoadingIsPossible(nextTransactionState) shouldBe true
      }
    }

    tc.test()
  }

  it should "load fast if next 3 states are ordered after prev state from the same master" in {
    val tc = new FastLoaderTestContainer {
      val partition = 0
      val masterID = 0
      val orderID = 0
      lastTransactionsMap(0) = TransactionState(LocalGeneratorCreator.getTransaction(), partition, masterID, orderID, 1, TransactionStatus.checkpointed, -1)
      val nextTransactionState = List(
        TransactionState(LocalGeneratorCreator.getTransaction(), partition, masterID, orderID + 1, 1, TransactionStatus.checkpointed, -1),
        TransactionState(LocalGeneratorCreator.getTransaction(), partition, masterID, orderID + 2, 1, TransactionStatus.checkpointed, -1),
        TransactionState(LocalGeneratorCreator.getTransaction(), partition, masterID, orderID + 3, 1, TransactionStatus.checkpointed, -1))

      override def test(): Unit = {
        fastLoader.checkIfTransactionLoadingIsPossible(nextTransactionState) shouldBe true
      }
    }

    tc.test()
  }

  it should "not load fast if next 3 states are not strictly ordered after prev state from the same master" in {
    val tc = new FastLoaderTestContainer {
      val partition = 0
      val masterID = 0
      val orderID = 0
      lastTransactionsMap(0) = TransactionState(LocalGeneratorCreator.getTransaction(), partition, masterID, orderID, 1, TransactionStatus.checkpointed, -1)
      val nextTransactionState = List(
        TransactionState(LocalGeneratorCreator.getTransaction(), partition, masterID, orderID + 1, 1, TransactionStatus.checkpointed, -1),
        TransactionState(LocalGeneratorCreator.getTransaction(), partition, masterID, orderID + 2, 1, TransactionStatus.checkpointed, -1),
        TransactionState(LocalGeneratorCreator.getTransaction(), partition, masterID, orderID + 2, 1, TransactionStatus.checkpointed, -1))

      override def test(): Unit = {
        fastLoader.checkIfTransactionLoadingIsPossible(nextTransactionState) shouldBe false
      }
    }

    tc.test()
  }

  it should "not load fast if state after prev is not ordered" in {
    val tc = new FastLoaderTestContainer {
      val partition = 0
      val masterID = 0
      val orderID = 0
      lastTransactionsMap(0) = TransactionState(LocalGeneratorCreator.getTransaction(), partition, masterID, orderID, 1, TransactionStatus.checkpointed, -1)
      val nextTransactionState = List(
        TransactionState(LocalGeneratorCreator.getTransaction(), partition, masterID, orderID, 1, TransactionStatus.checkpointed, -1))

      override def test(): Unit = {
        fastLoader.checkIfTransactionLoadingIsPossible(nextTransactionState) shouldBe false
      }
    }

    tc.test()
  }

  it should "not load fast if state after prev is ordered but master differs" in {
    val tc = new FastLoaderTestContainer {
      val partition = 0
      val masterID = 0
      val orderID = 0
      lastTransactionsMap(0) = TransactionState(LocalGeneratorCreator.getTransaction(), partition, masterID, orderID, 1, TransactionStatus.checkpointed, -1)
      val nextTransactionState = List(
        TransactionState(LocalGeneratorCreator.getTransaction(), partition, masterID + 1, orderID + 1, 1, TransactionStatus.checkpointed, -1))

      override def test(): Unit = {
        fastLoader.checkIfTransactionLoadingIsPossible(nextTransactionState) shouldBe false
      }
    }

    tc.test()
  }

  it should "not load fast if next 3 states are strictly ordered after prev state from not the same master" in {
    val tc = new FastLoaderTestContainer {
      val partition = 0
      val masterID = 0
      val orderID = 0
      lastTransactionsMap(0) = TransactionState(LocalGeneratorCreator.getTransaction(), partition, masterID + 1, orderID, 1, TransactionStatus.checkpointed, -1)
      val nextTransactionState = List(
        TransactionState(LocalGeneratorCreator.getTransaction(), partition, masterID, orderID + 1, 1, TransactionStatus.checkpointed, -1),
        TransactionState(LocalGeneratorCreator.getTransaction(), partition, masterID, orderID + 2, 1, TransactionStatus.checkpointed, -1),
        TransactionState(LocalGeneratorCreator.getTransaction(), partition, masterID, orderID + 2, 1, TransactionStatus.checkpointed, -1))

      override def test(): Unit = {
        fastLoader.checkIfTransactionLoadingIsPossible(nextTransactionState) shouldBe false
      }
    }

    tc.test()
  }

  it should "not load fast if next 3 states are strictly ordered after prev state from not the same master - case 2" in {
    val tc = new FastLoaderTestContainer {
      val partition = 0
      val masterID = 0
      val orderID = 0
      lastTransactionsMap(0) = TransactionState(LocalGeneratorCreator.getTransaction(), partition, masterID, orderID, 1, TransactionStatus.checkpointed, -1)
      val nextTransactionState = List(
        TransactionState(LocalGeneratorCreator.getTransaction(), partition, masterID, orderID + 1, 1, TransactionStatus.checkpointed, -1),
        TransactionState(LocalGeneratorCreator.getTransaction(), partition, masterID, orderID + 2, 1, TransactionStatus.checkpointed, -1),
        TransactionState(LocalGeneratorCreator.getTransaction(), partition, masterID + 1, orderID + 3, 1, TransactionStatus.checkpointed, -1))

      override def test(): Unit = {
        fastLoader.checkIfTransactionLoadingIsPossible(nextTransactionState) shouldBe false
      }
    }

    tc.test()
  }

  it should "load one transaction if check is ok" in {
    val tc = new FastLoaderTestContainer {
      val partition = 0
      val masterID = 0
      val orderID = 0
      lastTransactionsMap(0) = TransactionState(LocalGeneratorCreator.getTransaction(), partition, masterID, orderID, 1, TransactionStatus.checkpointed, -1)
      val nextTransactionState = List(
        TransactionState(LocalGeneratorCreator.getTransaction(), partition, masterID, orderID + 1, 1, TransactionStatus.checkpointed, -1))
      var ctr: Int = 0
      val l = new CountDownLatch(1)

      override def test(): Unit = {
        fastLoader.load(nextTransactionState, new FastLoaderOperatorTestImpl, new FirstFailLockableTaskExecutor("lf"), (consumer: TransactionOperator, transaction: ConsumerTransaction) => {
          ctr += 1
          l.countDown()
        })
        l.await()
        ctr shouldBe 1
        lastTransactionsMap(0).transactionID shouldBe nextTransactionState.head.transactionID
      }
    }

    tc.test()
  }

  it should "load three transactions if check is ok" in {
    val tc = new FastLoaderTestContainer {
      val partition = 0
      val masterID = 0
      val orderID = 0
      lastTransactionsMap(0) = TransactionState(LocalGeneratorCreator.getTransaction(), partition, masterID, orderID, 1, TransactionStatus.checkpointed, -1)
      val nextTransactionState = List(
        TransactionState(LocalGeneratorCreator.getTransaction(), partition, masterID, orderID + 1, 1, TransactionStatus.checkpointed, -1),
        TransactionState(LocalGeneratorCreator.getTransaction(), partition, masterID, orderID + 2, 1, TransactionStatus.checkpointed, -1),
        TransactionState(LocalGeneratorCreator.getTransaction(), partition, masterID, orderID + 3, 1, TransactionStatus.checkpointed, -1))
      var ctr: Int = 0
      val l = new CountDownLatch(1)

      override def test(): Unit = {
        fastLoader.load(nextTransactionState, new FastLoaderOperatorTestImpl, new FirstFailLockableTaskExecutor("lf"), (consumer: TransactionOperator, transaction: ConsumerTransaction) => {
          ctr += 1
          if (ctr == 3)
            l.countDown()
        })
        l.await()
        ctr shouldBe 3
        lastTransactionsMap(0).transactionID shouldBe nextTransactionState.last.transactionID
      }
    }

    tc.test()
  }

}
