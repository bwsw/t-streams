package agents.subscriber

import java.util.UUID

import com.bwsw.tstreams.agents.consumer.subscriber_v2.{Callback, QueueBuilder, ProcessingEngine}
import com.bwsw.tstreams.agents.consumer.{Transaction, TransactionOperator}
import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor
import com.datastax.driver.core.utils.UUIDs
import org.scalatest.{Matchers, FlatSpec}

import scala.collection.mutable.ListBuffer

class ProcessingEngineOperatorTestImpl extends TransactionOperator[String] {
  val TOTAL = 10
  val txns = new ListBuffer[Transaction[String]]()
  for(i <- 0 until TOTAL)
    txns += new Transaction[String](0, UUIDs.timeBased(), 1, -1)

  override def getLastTransaction(partition: Int): Option[Transaction[String]] = None

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
  it should "create Processing engine" in {
    val q = new QueueBuilder.InMemory().generateQueueObject(0)
    val pe = new ProcessingEngine[String](new ProcessingEngineOperatorTestImpl(), Set[Int](0), q, new Callback[String] {
      override def onEvent(consumer: TransactionOperator[String], partition: Int, uuid: UUID, count: Int): Unit = {

      }
    }, new FirstFailLockableTaskExecutor("lf"))
  }

}
