package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.UUID

import com.bwsw.tstreams.agents.consumer.TransactionOperator
import com.bwsw.tstreams.agents.consumer.subscriber.QueueBuilder.QueueItemType
import com.bwsw.tstreams.common.{FirstFailLockableTaskExecutor, UUIDComparator}
import com.bwsw.tstreams.coordination.messages.state.TransactionStatus

/**
  * Created by Ivan Kudryavtsev on 22.08.16.
  * Loads transactions in full from database if fast loader is unable to load them
  */
class TransactionFullLoader(partitions: Set[Int],
                            lastTransactionsMap: ProcessingEngine.LastTransactionStateMapType)
  extends AbstractTransactionLoader {

  /**
    * checks if possible to do full loading
    *
    * @param seq
    * @return
    */
  override def checkIfTransactionLoadingIsPossible(seq: QueueItemType): Boolean = {
    val last = seq.last
    if (UUIDComparator.compare(last.uuid, lastTransactionsMap(last.partition).uuid) != 1) {
      Subscriber.logger.warn(s"Last UUID: ${last.uuid}, In DB UUID: ${lastTransactionsMap(last.partition).uuid}")
      return false
    }
    true
  }

  /**
    * loads transactions to callback
    *
    * @param seq      sequence to load (we need the last one)
    * @param consumer consumer which loads
    * @param executor executor which adds to callback
    * @param callback callback which handles
    * @tparam T
    */
  override def load[T](seq: QueueItemType,
                       consumer: TransactionOperator[T],
                       executor: FirstFailLockableTaskExecutor,
                       callback: Callback[T]): Unit = {
    val last = seq.last
    val first: UUID = lastTransactionsMap(last.partition).uuid
    val data = consumer.getTransactionsFromTo(last.partition, first, last.uuid)
    data.foreach(elt =>
      executor.submit(s"<CallbackTask#Full>", new ProcessingEngine.CallbackTask[T](consumer,
        TransactionState(elt.getTransactionUUID(), last.partition, -1, -1, elt.getCount(), TransactionStatus.postCheckpoint, -1), callback)))

    if (data.nonEmpty)
      lastTransactionsMap(last.partition) = TransactionState(data.last.getTransactionUUID(), last.partition, last.masterSessionID, last.queueOrderID, data.last.getCount(), TransactionStatus.postCheckpoint, -1)
  }
}
