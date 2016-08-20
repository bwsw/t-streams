package com.bwsw.tstreams.agents.consumer.subscriber_v2

import com.bwsw.tstreams.agents.consumer.Consumer
import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor

import scala.collection.mutable

/**
  * Created by Ivan Kudryavtsev on 20.08.16.
  */
class ProcessingEngine[T](consumer: Consumer[T],
                          queue: QueueBuilder.QueueType,
                          callback: Callback[T],
                          executor: FirstFailLockableTaskExecutor) {

  // keeps last transaction states processed
  val lastTransactionsMap = mutable.Map[Int, TransactionState]()


  /**
    * allows to load data fast without database calls
    * @param seq
    */
  def loadFast(seq: QueueBuilder.QueueItemType) = {
    seq foreach(elt =>
      executor.submit(new ProcessingEngine.CallbackTask[T](consumer, elt, callback)))
    val last = seq.last
    lastTransactionsMap(last.partition) = last
  }

  /**
    * Checks if seq can be load fast without additional calls to database
    * @param seq
    * @return
    */
  def checkCanBeLoadFast(seq: QueueBuilder.QueueItemType): Boolean = {
    val first = seq.head
    // if there is no last for partition, then no info
    if(!lastTransactionsMap.contains(first.partition))
      return false
    val prev = lastTransactionsMap(first.partition)
    checkListSeq(prev, seq, compareIfStrictlySequential)
  }

  /**
    * utility function with compares head and tail of the list to find if they satisfy some condition
    * @param head
    * @param l
    * @return
    */
  private def checkListSeq(head: TransactionState,
                           l: QueueBuilder.QueueItemType ,
                           predicate: (TransactionState, TransactionState) => Boolean): Boolean = (head, l) match {
    case (_, Nil, _) => true
    case (head, e :: l, _) =>
      predicate(head, e) && checkListSeq(e, l, predicate)
  }

  /**
    * checks that two items satisfy load fast condition
    * @param e1
    * @param e2
    * @return
    */
  private def compareIfStrictlySequential(e1: TransactionState, e2: TransactionState): Boolean =
    (e1.masterSessionID == e2.masterSessionID) &&
      (e2.queueOrderID - e1.queueOrderID == 1)


}


object ProcessingEngine {
  class CallbackTask[T](consumer: Consumer[T], transactionState: TransactionState, callback: Callback[T]) extends Runnable {
    override def run(): Unit = {
      callback.onEvent(consumer = consumer, partition = transactionState.partition, uuid = transactionState.uuid, count = transactionState.itemCount)
    }
  }
}