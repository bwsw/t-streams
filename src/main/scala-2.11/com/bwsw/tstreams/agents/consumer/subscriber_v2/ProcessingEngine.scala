package com.bwsw.tstreams.agents.consumer.subscriber_v2

import java.util.UUID
import java.util.concurrent.TimeUnit

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
  val lastTransactionsEventsMap = mutable.Map[Int, Long]()
  val fastLoader = new TransactionStateFastLoader(partitions, lastTransactionsMap)


  val consumerPartitions = consumer.getPartitions()

  if(!partitions.subsetOf(consumerPartitions))
    throw new IllegalArgumentException("Partition set which is used in ProcessingEngine is not subset of Consumer's partitions.")

  partitions foreach (p => {
    lastTransactionsEventsMap(p)  = System.currentTimeMillis()
    lastTransactionsMap(p)        = TransactionState(consumer.getCurrentOffset(p), p, -1, -1, -1, TransactionStatus.postCheckpoint, -1) })



  /**
    * loads from C*
    * @param seq
    */
  def loadFull(seq: QueueBuilder.QueueItemType) = {
    val last = seq.last
    val first: UUID = lastTransactionsMap(last.partition).uuid
    val data = consumer.getTransactionsFromTo(last.partition, first, last.uuid)

    data foreach(elt =>
      executor.submit(new ProcessingEngine.CallbackTask[T](consumer,
        TransactionState(elt.getTxnUUID(), last.partition, -1, -1, elt.getCount(), TransactionStatus.postCheckpoint, -1), callback)))

    if (data.size > 0)
      lastTransactionsMap(last.partition) = TransactionState(data.last.getTxnUUID(), last.partition, -1, -1, data.last.getCount(), TransactionStatus.postCheckpoint, -1)
  }



  /**
    * Checks if seq can be load fast without additional calls to database
    * @param seq
    * @return
    */
  def checkCanBeLoadFull(seq: QueueBuilder.QueueItemType): Boolean = {
    val last = seq.last
    val uuidComparator = new UUIDComparator()
    // if there is no last for partition, then no info
    if(uuidComparator.compare(last.uuid, lastTransactionsMap(last.partition).uuid) != 1)
      return false
    true
  }

  def handleQueue(pollTimeMs: Int) = {
    val seq = queue.get(pollTimeMs, TimeUnit.MILLISECONDS)
    if(seq != null) {
      if(seq.size > 0) {
        if(fastLoader.checkCanBeLoadFast(seq))
          fastLoader.loadFast[T](seq, consumer, executor, callback)
        else if (checkCanBeLoadFull(seq))
          loadFull(seq)
        lastTransactionsEventsMap(seq.head.partition) = System.currentTimeMillis()
      }
    }
    partitions foreach (p => if (System.currentTimeMillis() - lastTransactionsEventsMap(p) > pollTimeMs) enqueueLastTransactionFromDB(p))
  }




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
    if(uuidComparator.compare(t.get.getTxnUUID(), lastTransactionsMap(partition).uuid) != 1)
      return

    val r = Random
    val tl = List(TransactionState(uuid             = t.get.getTxnUUID(),
                                    partition       = partition,
                                    masterSessionID = r.nextInt(),
                                    queueOrderID    =  r.nextInt(),
                                    itemCount       = t.get.getCount(), state = TransactionStatus.postCheckpoint,
                                    ttl             = -1))
    queue.put(tl)
    lastTransactionsEventsMap(partition) = System.currentTimeMillis()
  }
}


object ProcessingEngine {
  class CallbackTask[T](consumer: Consumer[T], transactionState: TransactionState, callback: Callback[T]) extends Runnable {
    override def run(): Unit = {
      callback.onEvent(consumer = consumer, partition = transactionState.partition, uuid = transactionState.uuid, count = transactionState.itemCount)
    }
  }
}