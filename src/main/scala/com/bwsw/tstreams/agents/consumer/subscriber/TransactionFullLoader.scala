package com.bwsw.tstreams.agents.consumer.subscriber

import com.bwsw.tstreams.agents.consumer.TransactionOperator
import com.bwsw.tstreams.agents.consumer.subscriber.QueueBuilder.QueueItemType
import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor
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
    if (last.transactionID <= lastTransactionsMap(last.partition).transactionID) {
      Subscriber.logger.warn(s"Last ID: ${last.transactionID}, In DB ID: ${lastTransactionsMap(last.partition).transactionID}")
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
    val first = lastTransactionsMap(last.partition).transactionID
    val data = consumer.getTransactionsFromTo(last.partition, first, last.transactionID)
    data.foreach(elt =>
      executor.submit(s"<CallbackTask#Full>", new ProcessingEngine.CallbackTask[T](consumer,
        TransactionState(elt.getTransactionID(), last.partition, -1, -1, elt.getCount(), TransactionStatus.checkpointed, -1), callback)))

    if (data.nonEmpty)
      lastTransactionsMap(last.partition) = TransactionState(data.last.getTransactionID(), last.partition, last.masterSessionID, last.queueOrderID, data.last.getCount(), TransactionStatus.checkpointed, -1)
  }
}
