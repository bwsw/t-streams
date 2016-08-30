package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.concurrent.TimeUnit

import com.bwsw.tstreams.agents.consumer.TransactionOperator
import com.bwsw.tstreams.common.{FirstFailLockableTaskExecutor, UUIDComparator}
import com.bwsw.tstreams.coordination.messages.state.TransactionStatus
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.Random
import scala.util.Random

/**
  * Created by Ivan Kudryavtsev on 20.08.16.
  * Does top-level management tasks for new events.
  */
class ProcessingEngine[T](consumer: TransactionOperator[T],
                          partitions: Set[Int],
                          queueBuilder: QueueBuilder.Abstract,
                          callback: Callback[T]) {

  private val id = Math.abs(Random.nextInt())
  // keeps last transaction states processed
  private val lastTransactionsMap = mutable.Map[Int, TransactionState]()
  private val lastTransactionsEventsMap = mutable.Map[Int, Long]()
  private val executor = new FirstFailLockableTaskExecutor(s"pe-${id}-executor")
  private val queue = queueBuilder.generateQueueObject(Math.abs(Random.nextInt()))
  private var isFirstTime = true


  ProcessingEngine.logger.info(s"Processing engine ${id} will serve ${partitions}.")

  def getExecutor() = executor
  def getQueue():QueueBuilder.QueueType = queue

  def getLastPartitionActivity(partition: Int): Long = lastTransactionsEventsMap(partition)
  def setLastPartitionActivity(partition: Int): Unit = lastTransactionsEventsMap(partition) = System.currentTimeMillis()
  def getLastTransactionHandled(partition: Int) = lastTransactionsMap(partition)

  // loaders
  val fastLoader = new TransactionFastLoader(partitions, lastTransactionsMap)
  val fullLoader = new TransactionFullLoader(partitions, lastTransactionsMap)

  val consumerPartitions = consumer.getPartitions()

  if(!partitions.subsetOf(consumerPartitions))
    throw new IllegalArgumentException("PE ${id} - Partition set which is used in ProcessingEngine is not subset of Consumer's partitions.")

  partitions foreach (p => {
    setLastPartitionActivity(p)
    lastTransactionsMap(p) = TransactionState(consumer.getCurrentOffset(p), p, -1, -1, -1, TransactionStatus.postCheckpoint, -1) })

  /**
    * Reads transactions from database or fast and does self-kick if no events.
    * @param pollTimeMs
    */
  def handleQueue(pollTimeMs: Int) = {
    val seq = queue.get(pollTimeMs, TimeUnit.MILLISECONDS)
    if(seq != null) {
      if(seq.size > 0) {
        if(fastLoader.checkIfPossible(seq)) {
          fastLoader.load[T](seq, consumer, executor, callback)
        }
        else if (fullLoader.checkIfPossible(seq)) {
          ProcessingEngine.logger.info(s"PE ${id} - Load full occured for seq ${seq}")
          fullLoader.load[T](seq, consumer, executor, callback)
        }
        setLastPartitionActivity(seq.head.partition)
      }
    }


    partitions
      .foreach(p =>
        if (isFirstTime || (System.currentTimeMillis() - getLastPartitionActivity(p) > pollTimeMs)) {
          ProcessingEngine.logger.debug(s"PE ${id} - No events during polling interval for partition ${p}, will do enqueuing from DB.")
          enqueueLastTransactionFromDB(p)
        })

    isFirstTime = false
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

    val tl = List(TransactionState(uuid             = t.get.getTxnUUID(),
                                    partition       = partition,
                                    masterSessionID = 0,
                                    queueOrderID    = 0,
                                    itemCount       = t.get.getCount(), state = TransactionStatus.postCheckpoint,
                                    ttl             = -1))

    if(Subscriber.logger.isDebugEnabled())
      Subscriber.logger.debug(s"Enqueued ${tl}")

    queue.put(tl)
  }

  def stop() = {
    executor.shutdownOrDie(100, TimeUnit.SECONDS)
  }
}


object ProcessingEngine {
  val logger = LoggerFactory.getLogger(this.getClass)

  type LastTransactionStateMapType = mutable.Map[Int, TransactionState]
  class CallbackTask[T](consumer: TransactionOperator[T], transactionState: TransactionState, callback: Callback[T]) extends Runnable {
    override def run(): Unit = {
      callback.onEvent(consumer = consumer, partition = transactionState.partition, uuid = transactionState.uuid, count = transactionState.itemCount)
    }
  }
}