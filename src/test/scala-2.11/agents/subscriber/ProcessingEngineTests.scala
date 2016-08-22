package agents.subscriber

import java.util.UUID
import java.util.concurrent.TimeUnit

import com.bwsw.tstreams.agents.consumer.subscriber_v2.{TransactionState, Callback, QueueBuilder, ProcessingEngine}
import com.bwsw.tstreams.agents.consumer.{Transaction, TransactionOperator}
import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor
import com.bwsw.tstreams.coordination.messages.state.TransactionStatus
import com.datastax.driver.core.utils.UUIDs
import org.scalatest.{Matchers, FlatSpec}

import scala.collection.mutable.ListBuffer

class ProcessingEngineOperatorTestImpl extends TransactionOperator[String] {
  val TOTAL = 10
  val txns = new ListBuffer[Transaction[String]]()
  for(i <- 0 until TOTAL)
    txns += new Transaction[String](0, UUIDs.timeBased(), 1, -1)

  var lstTransaction: Option[Transaction[String]] = None

  override def getLastTransaction(partition: Int): Option[Transaction[String]] = lstTransaction

  override def getTransactionById(partition: Int, uuid: UUID): Option[Transaction[String]] = None

  override def setStreamPartitionOffset(partition: Int, uuid: UUID): Unit = {}

  override def updateTransactionInfoFromDB(txn: UUID, partition: Int): Option[Transaction[String]] = None

  override def getTransactionsFromTo(partition: Int, from: UUID, to: UUID): ListBuffer[Transaction[String]] =
    txns

  override def checkpoint(): Unit = {}

  override def getPartitions(): Set[Int] = Set[Int](0)

  override def getCurrentOffset(partition: Int): UUID = UUIDs.timeBased()
}

/**
  * Created by Ivan Kudryavtsev on 22.08.16.
  */
class ProcessingEngineTests extends FlatSpec with Matchers {

  val cb = new Callback[String] {
    override def onEvent(consumer: TransactionOperator[String], partition: Int, uuid: UUID, count: Int): Unit = {}
  }

  val e = new FirstFailLockableTaskExecutor("lf")
  val q = new QueueBuilder.InMemory().generateQueueObject(0)


  "constructor" should "create Processing engine" in {
    val pe = new ProcessingEngine[String](new ProcessingEngineOperatorTestImpl(), Set[Int](0), q, cb, e)
  }

  "enqueueLastTransactionFromDB" should "not Enqueue last transaction state to Queue if it's not defined" in {
    val pe = new ProcessingEngine[String](new ProcessingEngineOperatorTestImpl(), Set[Int](0), q, cb, e)
    pe.enqueueLastTransactionFromDB(0)
    val elt = q.get(200, TimeUnit.MILLISECONDS)
    elt shouldBe null
  }

  "enqueueLastTransactionFromDB" should "enqueue last transaction state to Queue if it's newer than we have in our database" in {
    val c = new ProcessingEngineOperatorTestImpl()
    val pe = new ProcessingEngine[String](c, Set[Int](0), q, cb, e)
    c.lstTransaction = Option[Transaction[String]](new Transaction(0, UUIDs.timeBased(), 1, -1))
    pe.enqueueLastTransactionFromDB(0)
    val elt = q.get(200, TimeUnit.MILLISECONDS)
    elt.head.uuid shouldBe c.lstTransaction.get.getTxnUUID()
  }

  "enqueueLastTransactionFromDB" should "not enqueue last transaction state to Queue if it's older than we have in our database" in {
    val c = new ProcessingEngineOperatorTestImpl()
    c.lstTransaction = Option[Transaction[String]](new Transaction(0, UUIDs.timeBased(), 1, -1))
    val pe = new ProcessingEngine[String](c, Set[Int](0), q, cb, e)
    pe.enqueueLastTransactionFromDB(0)
    val elt = q.get(200, TimeUnit.MILLISECONDS)
    elt shouldBe null
  }


}
