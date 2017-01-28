package agents.subscriber

import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.consumer.subscriber.{Callback, TransactionFullLoader, TransactionState}
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor
import com.bwsw.tstreams.coordination.messages.state.TransactionStatus
import org.scalatest.{FlatSpec, Matchers}
import testutils.LocalGeneratorCreator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class FullLoaderOperatorTestImpl extends TransactionOperator[String] {

  val TOTAL = 10
  val transactions = new ListBuffer[ConsumerTransaction[String]]()
  for (i <- 0 until TOTAL)
    transactions += new ConsumerTransaction[String](0, LocalGeneratorCreator.getTransaction(), 1, -1)

  override def getLastTransaction(partition: Int): Option[ConsumerTransaction[String]] = None

  override def getTransactionById(partition: Int, id: Long): Option[ConsumerTransaction[String]] = None

  override def setStreamPartitionOffset(partition: Int, id: Long): Unit = {}

  override def loadTransactionFromDB(partition: Int, transaction: Long): Option[ConsumerTransaction[String]] = None

  override def getTransactionsFromTo(partition: Int, from: Long, to: Long): ListBuffer[ConsumerTransaction[String]] =
    transactions

  override def checkpoint(): Unit = {}

  override def getPartitions(): Set[Int] = Set[Int](0)

  override def getCurrentOffset(partition: Int) = LocalGeneratorCreator.getTransaction()

  override def buildTransactionObject(partition: Int, id: Long, count: Int): Option[ConsumerTransaction[String]] = Some(new ConsumerTransaction[String](0, LocalGeneratorCreator.getTransaction(), 1, -1))

  override def getProposedTransactionId(): Long = LocalGeneratorCreator.getTransaction()
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

  val gen = LocalGeneratorCreator.getGen()

  it should "load if next state is after prev state by id" in {
    val tc = new FullLoaderTestContainer {
      val partition = 0
      val masterID = 0
      val orderID = 0
      lastTransactionsMap(0) = TransactionState(LocalGeneratorCreator.getTransaction(), partition, masterID, orderID, 1, TransactionStatus.checkpointed, -1)
      val nextTransactionState = List(TransactionState(LocalGeneratorCreator.getTransaction(), partition, masterID, orderID + 1, 1, TransactionStatus.checkpointed, -1))

      override def test(): Unit = {
        fullLoader.checkIfTransactionLoadingIsPossible(nextTransactionState) shouldBe true
      }
    }

    tc.test()
  }

  it should "not load  if next state is before prev state by id" in {
    val tc = new FullLoaderTestContainer {
      val partition = 0
      val masterID = 0
      val orderID = 0
      val nextTransactionState = List(TransactionState(LocalGeneratorCreator.getTransaction(), partition, masterID, orderID + 1, 1, TransactionStatus.checkpointed, -1))
      lastTransactionsMap(0) = TransactionState(LocalGeneratorCreator.getTransaction(), partition, masterID, orderID, 1, TransactionStatus.checkpointed, -1)

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
      lastTransactionsMap(0) = TransactionState(LocalGeneratorCreator.getTransaction(), partition, masterID, orderID, 1, TransactionStatus.checkpointed, -1)
      val fullLoader2 = new TransactionFullLoader(partitions(), lastTransactionsMap)
      val consumerOuter = new FullLoaderOperatorTestImpl()
      val nextTransactionState = List(TransactionState(consumerOuter.transactions.last.getTransactionID(), partition, masterID, orderID + 1, 1, TransactionStatus.checkpointed, -1))

      override def test(): Unit = {
        var ctr: Int = 0
        val l = new CountDownLatch(1)
        fullLoader2.load[String](nextTransactionState,
          consumerOuter,
          new FirstFailLockableTaskExecutor("lf"),
          new Callback[String] {
            override def onTransaction(consumer: TransactionOperator[String], transaction: ConsumerTransaction[String]): Unit = {
              ctr += 1
              if (ctr == consumerOuter.TOTAL)
                l.countDown()
            }
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
      lastTransactionsMap(0) = TransactionState(LocalGeneratorCreator.getTransaction(), partition, masterID, orderID, 1, TransactionStatus.checkpointed, -1)
      val fullLoader2 = new TransactionFullLoader(partitions(), lastTransactionsMap)
      val consumerOuter = new FullLoaderOperatorTestImpl()
      val nextTransactionState = List(TransactionState(consumerOuter.transactions.last.getTransactionID(), partition, masterID, orderID + 1, 1, TransactionStatus.checkpointed, -1))

      override def test(): Unit = {
        var ctr: Int = 0
        val l = new CountDownLatch(1)
        fullLoader2.load[String](nextTransactionState,
          consumerOuter,
          new FirstFailLockableTaskExecutor("lf"),
          new Callback[String] {
            override def onTransaction(consumer: TransactionOperator[String], transaction: ConsumerTransaction[String]): Unit = {
              ctr += 1
              if (ctr == consumerOuter.TOTAL)
                l.countDown()
            }
          })
        l.await(1, TimeUnit.SECONDS)
        ctr shouldBe consumerOuter.TOTAL
        lastTransactionsMap(0).transactionID shouldBe consumerOuter.transactions.last.getTransactionID()
      }
    }

    tc.test()
  }
}
