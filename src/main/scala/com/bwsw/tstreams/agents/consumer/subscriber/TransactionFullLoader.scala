package com.bwsw.tstreams.agents.consumer.subscriber

import com.bwsw.tstreams.agents.consumer.subscriber.QueueBuilder.QueueItemType
import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor
import com.bwsw.tstreams.proto.protocol.TransactionState
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates

import scala.collection.mutable.ListBuffer

object TransactionFullLoader {
  val EXPECTED_BUT_EMPTY_RESPONSE_DELAY = 1000
}

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
    */
  override def load(seq: QueueItemType,
                    consumer: TransactionOperator,
                    executor: FirstFailLockableTaskExecutor,
                    callback: Callback): Int = {
    val last = seq.last
    var first = lastTransactionsMap(last.partition).transactionID
    if (Subscriber.logger.isDebugEnabled())
      Subscriber.logger.debug(s"TransactionFullLoader.load: First: $first, last: ${last.transactionID}, ${last.transactionID - first}")
    var transactions = ListBuffer[ConsumerTransaction]()
    var flag = true
    var counter = 0
    while (flag) {
      counter += 1
      if(Subscriber.logger.isDebugEnabled())
        Subscriber.logger.debug(s"Load full begin (partition = ${last.partition}, first = $first, last = ${last.transactionID}, master = ${last.masterID})")
      val newTransactions = consumer.getTransactionsFromTo(last.partition, first, last.transactionID)
      transactions ++= newTransactions.filter(transaction => transaction.getState() != TransactionStates.Invalid)

      if(Subscriber.logger.isDebugEnabled())
        Subscriber.logger.debug(s"Load full end (partition = ${last.partition}, first = $first, last = ${last.transactionID}, master = ${last.masterID})")

      if (last.masterID > 0 && !last.isNotReliable) {
        // we wait for certain item
        // to switch to fast load next
        if (newTransactions.nonEmpty) {
          if (newTransactions.last.getTransactionID() == last.transactionID) {
            if(Subscriber.logger.isDebugEnabled())
              Subscriber.logger.debug(s"Load full completed (partition = ${last.partition}, first = $first, last = ${last.transactionID}, master = ${last.masterID}  )")
            flag = false
          }
          else
            first = newTransactions.last.getTransactionID()
        } else {
          /*
          to keep server ok from our activity add small delay
          it happens very rare case - when load full occurred and no data available after response
          so sleep a little bit to give data time to be gathered
          in normal, stationary situation it must not happen.
          */
          Thread.sleep(TransactionFullLoader.EXPECTED_BUT_EMPTY_RESPONSE_DELAY)
        }
      } else
        flag = false
    }

    if (Subscriber.logger.isDebugEnabled())
      Subscriber.logger.debug(s"Series received from the database:  $transactions")

    transactions.foreach(elt =>
      executor.submit(s"<CallbackTask#Full>", new ProcessingEngine.CallbackTask(consumer,
        TransactionState(transactionID = elt.getTransactionID(),
          partition = last.partition, masterID = -1, orderID = -1, count = elt.getCount(),
          status = TransactionState.Status.Checkpointed, ttlMs = -1), callback)))

    if (transactions.nonEmpty)
      lastTransactionsMap(last.partition) = TransactionState(transactionID = transactions.last.getTransactionID(),
        partition = last.partition, masterID = last.masterID, orderID = last.orderID,
        count = transactions.last.getCount(), status = TransactionState.Status.Checkpointed, ttlMs = -1)

    transactions.size
  }

}
