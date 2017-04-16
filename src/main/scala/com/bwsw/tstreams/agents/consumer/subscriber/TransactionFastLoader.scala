package com.bwsw.tstreams.agents.consumer.subscriber


import java.util.concurrent.atomic.AtomicInteger

import com.bwsw.tstreams.agents.consumer.TransactionOperator
import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor

/**
  * Created by Ivan Kudryavtsev on 21.08.16.
  */
class TransactionFastLoader(partitions: Set[Int],
                            lastTransactionsMap: ProcessingEngine.LastTransactionStateMapType) extends AbstractTransactionLoader {

  /**
    * checks that two items satisfy load fast condition
    *
    * @param e1
    * @param e2
    * @return
    */
  private def compareIfStrictlySequentialFast(e1: TransactionState, e2: TransactionState): Boolean =
    (e1.masterSessionID == e2.masterSessionID) &&
      (e2.queueOrderID - e1.queueOrderID == 1) &&
      partitions.contains(e2.partition)


  /**
    * utility function with compares head and tail of the list to find if they satisfy some condition
    *
    * @param head
    * @param l
    * @return
    */
  private def checkListSeq(head: TransactionState,
                           l: QueueBuilder.QueueItemType,
                           predicate: (TransactionState, TransactionState) => Boolean): Boolean = (head, l, predicate) match {
    case (_, Nil, _) => true
    case (_, elt :: tail, _) =>
      predicate(head, elt) && checkListSeq(elt, tail, predicate)
  }

  /**
    * Checks if seq can be load fast without additional calls to database
    *
    * @param seq
    * @return
    */
  override def checkIfTransactionLoadingIsPossible(seq: QueueBuilder.QueueItemType): Boolean = {
    val first = seq.head
    // if there is no last for partition, then no info

    val prev = lastTransactionsMap(first.partition)
    checkListSeq(prev, seq, compareIfStrictlySequentialFast)
  }


  val counter = new AtomicInteger(0)
  /**
    * allows to load data fast without database calls
    *
    * @param seq
    */
  override def load(seq: QueueBuilder.QueueItemType,
                    consumer: TransactionOperator,
                    executor: FirstFailLockableTaskExecutor,
                    callback: Callback): Int = {

    seq foreach (elt =>
      executor.submit(s"<CallbackTask#Fast>", new ProcessingEngine.CallbackTask(consumer, elt, callback)))

    val last = seq.last
    lastTransactionsMap(last.partition) = last
    seq.size
  }

}
