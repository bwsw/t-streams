package com.bwsw.tstreams.agents.consumer.subscriber_v2

import java.util.UUID
import java.util.concurrent.TimeUnit

import com.bwsw.tstreams.agents.consumer.{TransactionOperator, Consumer}
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

  // loaders
  val fastLoader = new TransactionFastLoader(partitions, lastTransactionsMap)
  val fullLoader = new TransactionFullLoader(partitions, lastTransactionsMap)

  val consumerPartitions = consumer.getPartitions()

  if(!partitions.subsetOf(consumerPartitions))
    throw new IllegalArgumentException("Partition set which is used in ProcessingEngine is not subset of Consumer's partitions.")

  partitions foreach (p => {
    lastTransactionsEventsMap(p)  = System.currentTimeMillis()
    lastTransactionsMap(p)        = TransactionState(consumer.getCurrentOffset(p), p, -1, -1, -1, TransactionStatus.postCheckpoint, -1) })


  def handleQueue(pollTimeMs: Int) = {
    val seq = queue.get(pollTimeMs, TimeUnit.MILLISECONDS)
    if(seq != null) {
      if(seq.size > 0) {
        if(fastLoader.checkIfPossible(seq)) {
          fastLoader.load[T](seq, consumer, executor, callback)
        }
        else if (fullLoader.checkIfPossible(seq)) {
          fullLoader.load[T](seq, consumer, executor, callback)
        }
        lastTransactionsEventsMap(seq.head.partition) = System.currentTimeMillis()
      }
    }
    partitions
      .foreach(p =>
        if (System.currentTimeMillis() - lastTransactionsEventsMap(p) > pollTimeMs)
          enqueueLastTransactionFromDB(p))
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
  type LastTransactionStateMapType = mutable.Map[Int, TransactionState]
  class CallbackTask[T](consumer: TransactionOperator[T], transactionState: TransactionState, callback: Callback[T]) extends Runnable {
    override def run(): Unit = {
      callback.onEvent(consumer = consumer, partition = transactionState.partition, uuid = transactionState.uuid, count = transactionState.itemCount)
    }
  }
}