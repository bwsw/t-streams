package agents.subscriber

import com.bwsw.tstreams.agents.consumer.subscriber_v2.{TransactionFullLoader, TransactionFastLoader, TransactionState}
import com.bwsw.tstreams.coordination.messages.state.TransactionStatus
import com.datastax.driver.core.utils.UUIDs
import org.scalatest.{Matchers, FlatSpec}

import scala.collection.mutable


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
  it should "load if next state is after prev state by uuid" in {
    val tc = new FullLoaderTestContainer {
      val partition = 0
      val masterID = 0
      val orderID = 0
      lastTransactionsMap(0) = TransactionState(UUIDs.timeBased(), partition, masterID, orderID, 1, TransactionStatus.postCheckpoint, -1)
      val nextTxnState = List(TransactionState(UUIDs.timeBased(), partition, masterID, orderID + 1, 1, TransactionStatus.postCheckpoint, -1))

      override def test(): Unit = {
        fullLoader.checkIfPossible(nextTxnState) shouldBe true
      }
    }

    tc.test()
  }

  it should "not load  if next state is before prev state by uuid" in {
    val tc = new FullLoaderTestContainer {
      val partition = 0
      val masterID = 0
      val orderID = 0
      val nextTxnState = List(TransactionState(UUIDs.timeBased(), partition, masterID, orderID + 1, 1, TransactionStatus.postCheckpoint, -1))
      lastTransactionsMap(0) = TransactionState(UUIDs.timeBased(), partition, masterID, orderID, 1, TransactionStatus.postCheckpoint, -1)

      override def test(): Unit = {
        fullLoader.checkIfPossible(nextTxnState) shouldBe false
      }
    }

    tc.test()
  }
}
