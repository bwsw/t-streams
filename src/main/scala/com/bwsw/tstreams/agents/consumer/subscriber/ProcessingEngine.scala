package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreams.agents.consumer.TransactionOperator
import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor
import com.bwsw.tstreams.coordination.messages.state.TransactionStatus
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.Random

/**
  * Created by Ivan Kudryavtsev on 20.08.16.
  * Does top-level management tasks for new events.
  */
class ProcessingEngine(consumer: TransactionOperator,
                          partitions: Set[Int],
                          queueBuilder: QueueBuilder.Abstract,
                          callback: Callback) {

  private val id = Math.abs(Random.nextInt())
  // keeps last transaction states processed
  private val lastTransactionsMap = mutable.Map[Int, TransactionState]()
  private val lastTransactionsEventsMap = mutable.Map[Int, Long]()
  private val executor = new FirstFailLockableTaskExecutor(s"pe-$id-executor")

  val isThresholdsSet = new AtomicBoolean(false)

  private val loadExecutor = new FirstFailLockableTaskExecutor(s"pe-$id-loadExecutor")
  private val queue = queueBuilder.generateQueueObject(Math.abs(Random.nextInt()))
  private var isFirstTime = true


  ProcessingEngine.logger.info(s"Processing engine $id will serve $partitions.")

  def getExecutor() = executor

  def getQueue() = queue

  def getLastPartitionActivity(partition: Int) = lastTransactionsEventsMap(partition)

  def setLastPartitionActivity(partition: Int): Unit = {
    lastTransactionsEventsMap(partition) = System.currentTimeMillis()
  }

  def getLastTransactionHandled(partition: Int) = lastTransactionsMap(partition)

  // loaders
  val fastLoader = new TransactionFastLoader(partitions, lastTransactionsMap)
  val fullLoader = new TransactionFullLoader(partitions, lastTransactionsMap)

  val consumerPartitions = consumer.getPartitions()

  if (!partitions.subsetOf(consumerPartitions))
    throw new IllegalArgumentException("PE ${id} - Partition set which is used in ProcessingEngine is not subset of Consumer's partitions.")

  partitions
    .foreach (p => {
      setLastPartitionActivity(p)
      lastTransactionsMap(p) = TransactionState(consumer.getCurrentOffset(p), p, -1, -1, -1, TransactionStatus.checkpointed, -1)
    })

  /**
    * Reads transactions from database or fast load and does self-kick if no events.
    *
    * @param pollTimeMs
    */
  def handleQueue(pollTimeMs: Int) = {
    if (!isThresholdsSet.get()) {
      isThresholdsSet.set(true)
      executor.setThresholds(queueLengthThreshold = 10, taskFullDelayThresholdMs = pollTimeMs + 50,
        taskDelayThresholdMs = pollTimeMs + 5, taskRunDelayThresholdMs = 50)

      loadExecutor.setThresholds(queueLengthThreshold = 1000, taskFullDelayThresholdMs = 150,
        taskDelayThresholdMs = 100, taskRunDelayThresholdMs = 50)
    }

    val seq = queue.get(pollTimeMs, TimeUnit.MILLISECONDS)
    if (seq != null) {
      isFirstTime = false
      if (seq.nonEmpty) {
        if (fastLoader.checkIfTransactionLoadingIsPossible(seq)) {
          fastLoader.load(seq, consumer, loadExecutor, callback)
        }
        else {
          if (fullLoader.checkIfTransactionLoadingIsPossible(seq)) {
            ProcessingEngine.logger.info(s"PE $id - Load full occurred for seq $seq")
            fullLoader.load(seq, consumer, loadExecutor, callback)
          } else {
            Subscriber.logger.warn(s"Fast and Full loading failed for $seq.")
          }
        }
        setLastPartitionActivity(seq.head.partition)
      }
    }


    partitions
      .foreach(p =>
        if (isFirstTime
          || (System.currentTimeMillis() - getLastPartitionActivity(p) > pollTimeMs && queue.getInFlight() == 0)
          || (System.currentTimeMillis() - getLastPartitionActivity(p) > pollTimeMs * 10)) {
          enqueueLastTransactionFromDB(p)
        })

    isFirstTime = false
  }


  /**
    * Enqueues in queue last transaction from database
    */
  def enqueueLastTransactionFromDB(partition: Int): Unit = {
    assert(partitions.contains(partition))

    val proposedTransactionId = consumer.getProposedTransactionId()

    val transactionStateList = List(TransactionState(
      transactionID   = proposedTransactionId,
      partition       = partition,
      masterSessionID = 0,
      queueOrderID    = 0,
      itemCount       = -1,
      state           = TransactionStatus.checkpointed,
      ttl             = -1))

    if (Subscriber.logger.isDebugEnabled())
      Subscriber.logger.debug(s"Enqueued $transactionStateList")

    queue.put(transactionStateList)
  }

  def stop() = {
    executor.shutdownOrDie(Subscriber.SHUTDOWN_WAIT_MAX_SECONDS, TimeUnit.SECONDS)
  }
}


object ProcessingEngine {
  val logger = LoggerFactory.getLogger(this.getClass)

  type LastTransactionStateMapType = mutable.Map[Int, TransactionState]

  class CallbackTask(consumer: TransactionOperator, transactionState: TransactionState, callback: Callback) extends Runnable {

    override def toString() = s"CallbackTask($transactionState)"

    override def run() = {
      callback.onTransactionCall(consumer = consumer, partition = transactionState.partition, transactionID = transactionState.transactionID, count = transactionState.itemCount)
    }
  }

}