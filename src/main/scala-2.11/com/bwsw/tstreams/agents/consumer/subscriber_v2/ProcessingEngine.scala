package com.bwsw.tstreams.agents.consumer.subscriber_v2

import com.bwsw.tstreams.agents.consumer.Consumer
import com.bwsw.tstreams.common.{FirstFailLockableTaskExecutor, UUIDComparator}
import com.bwsw.tstreams.coordination.messages.state.TransactionStatus

import scala.collection.mutable
import scala.util.Random

/**
  * Created by Ivan Kudryavtsev on 20.08.16.
  */
class ProcessingEngine[T](consumer: Consumer[T],
                          partitions: Set[Int],
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
    checkListSeq(prev, seq, compareIfStrictlySequentialFast)
  }

  /**
    * utility function with compares head and tail of the list to find if they satisfy some condition
    * @param head
    * @param l
    * @return
    */
  private def checkListSeq(head: TransactionState,
                           l: QueueBuilder.QueueItemType ,
                           predicate: (TransactionState, TransactionState) => Boolean): Boolean = (head, l, predicate) match {
    case (_, Nil, _) => true
    case (h, e :: l, p) =>
      predicate(h, e) && checkListSeq(e, l, p)
  }

  /**
    * checks that two items satisfy load fast condition
    * @param e1
    * @param e2
    * @return
    */
  private def compareIfStrictlySequentialFast(e1: TransactionState, e2: TransactionState): Boolean =
    (e1.masterSessionID == e2.masterSessionID) &&
      (e2.queueOrderID - e1.queueOrderID == 1) &&
      partitions.contains(e2.partition)

  /**
    * checks that two items satisfy load fast condition
    * @param e1
    * @param e2
    * @return
    */
  private def compareIfStrictlySequentialFull(e1: TransactionState, e2: TransactionState): Boolean =
    partitions.contains(e2.partition)



  /**
    * Enqueues in queue last transaction from Cassandra
    */

  def enqueueLastTransactionFromDB(partition: Int): Unit = {
    assert(partitions.contains(partition))

    val t = consumer.getLastTransaction(partition)
    if(!t.isDefined)
      return

    val uuidComparator = new UUIDComparator()

    // if current last transaction is newer than from db
    if(lastTransactionsMap.contains(partition) &&
      uuidComparator.compare(t.get.getTxnUUID(), lastTransactionsMap(partition).uuid) != 1)
      return

    val r = Random
    val tl = List(TransactionState(uuid             = t.get.getTxnUUID(),
                                    partition       = partition,
                                    masterSessionID = r.nextInt(),
                                    queueOrderID    =  r.nextInt(),
                                    itemCount       = t.get.getCount(), state = TransactionStatus.postCheckpoint,
                                    ttl             = -1))
    queue.put(tl)
  }

}


object ProcessingEngine {
  class CallbackTask[T](consumer: Consumer[T], transactionState: TransactionState, callback: Callback[T]) extends Runnable {
    override def run(): Unit = {
      callback.onEvent(consumer = consumer, partition = transactionState.partition, uuid = transactionState.uuid, count = transactionState.itemCount)
    }
  }
}