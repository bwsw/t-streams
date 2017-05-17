package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreams.agents.consumer.TransactionOperator
import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor
import com.bwsw.tstreams.proto.protocol.TransactionState
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
                       callback: Callback, pollingInterval: Int) {

  private val id = Math.abs(Random.nextInt())
  // keeps last transaction states processed
  private val lastTransactionsMap = mutable.Map[Int, TransactionState]()
  private val lastPartitionsEventsMap = mutable.Map[Int, Long]()

  private val isRunning = new AtomicBoolean(false)

  private val executor = new Thread(() => {
    while (isRunning.get)
      handleQueue(pollingInterval)
  }, s"pe-$id-executor")

  private val loadExecutor = new FirstFailLockableTaskExecutor(s"pe-$id-loadExecutor")

  val isThresholdsSet = new AtomicBoolean(false)

  private val queue = queueBuilder.generateQueueObject(Math.abs(Random.nextInt()))
  private var isFirstTime = true


  ProcessingEngine.logger.info(s"Processing engine $id will serve $partitions.")

  def getQueue() = queue

  def getLastPartitionActivity(partition: Int) = lastPartitionsEventsMap(partition)

  def setLastPartitionActivity(partition: Int) = {
    lastPartitionsEventsMap(partition) = System.currentTimeMillis()
  }

  def getLastTransactionHandled(partition: Int) = lastTransactionsMap(partition)

  // loaders
  val fastLoader = new TransactionFastLoader(partitions, lastTransactionsMap)
  val fullLoader = new TransactionFullLoader(partitions, lastTransactionsMap)

  val consumerPartitions = consumer.getPartitions

  if (!partitions.subsetOf(consumerPartitions))
    throw new IllegalArgumentException("PE ${id} - Partition set which is used in ProcessingEngine is not subset of Consumer's partitions.")

  partitions
    .foreach(p => {
      setLastPartitionActivity(p)
      lastTransactionsMap(p) = TransactionState(transactionID = consumer.getCurrentOffset(p),
        status = TransactionState.Status.Checkpointed, partition = p, masterID = -1,
        orderID = -1, count = -1, ttlMs = -1)
    })

  /**
    * Reads transactions from database or fast load and does self-kick if no events.
    *
    * @param pollTimeMs
    */
  def handleQueue(pollTimeMs: Int) = {

    if (!isThresholdsSet.get()) {
      isThresholdsSet.set(true)

      loadExecutor.setThresholds(queueLengthThreshold = 1000, taskFullDelayThresholdMs = 150,
        taskDelayThresholdMs = 100, taskRunDelayThresholdMs = 50)
    }

    var loadFullDataExist = false

    val seq = queue.get(pollTimeMs, TimeUnit.MILLISECONDS)

    if (Subscriber.logger.isDebugEnabled())
      Subscriber.logger.debug(s"$seq")

    if (seq != null) {
      isFirstTime = false
      if (seq.nonEmpty) {
        if (fastLoader.checkIfTransactionLoadingIsPossible(seq)) {
          fastLoader.load(seq, consumer, loadExecutor, callback)
        }
        else {
          if (fullLoader.checkIfTransactionLoadingIsPossible(seq)) {
            ProcessingEngine.logger.warn(s"PE $id - Load full occurred for seq $seq")
            if (fullLoader.load(seq, consumer, loadExecutor, callback) > 0)
              loadFullDataExist = true
          } else {
            Subscriber.logger.warn(s"Fast and Full loading failed for $seq.")
          }
        }
        setLastPartitionActivity(seq.head.partition)
      }
    }


    partitions
      .foreach(p =>
        if ((loadFullDataExist && queue.getInFlight() == 0)
          || isFirstTime
          || (System.currentTimeMillis() - getLastPartitionActivity(p) > pollTimeMs && queue.getInFlight() == 0)) {
          enqueueLastPossibleTransaction(p)
        })

    isFirstTime = false
  }


  /**
    * Enqueues in queue last transaction from database
    */
  def enqueueLastPossibleTransaction(partition: Int): Unit = {
    //println(s"Partition is: ${partition} in ${partitions}")

    assert(partitions.contains(partition))

    val proposedTransactionId = consumer.getProposedTransactionId

    val transactionStateList = List(TransactionState(
      transactionID = proposedTransactionId, partition = partition, masterID = 0, orderID = 0,
      count = -1, status = TransactionState.Status.Checkpointed, ttlMs = -1))

    if (Subscriber.logger.isDebugEnabled())
      Subscriber.logger.debug(s"Enqueued $transactionStateList")

    queue.put(transactionStateList)
  }

  def start() = {
    isRunning.set(true)
    executor.start()
  }

  def stop() = {
    isRunning.set(false)
    partitions.foreach(p => enqueueLastPossibleTransaction(p))
    executor.join(Subscriber.SHUTDOWN_WAIT_MAX_SECONDS * 1000)
  }
}


object ProcessingEngine {

  // val PROTECTION_INTERVAL = 10

  val logger = LoggerFactory.getLogger(this.getClass)

  type LastTransactionStateMapType = mutable.Map[Int, TransactionState]

  class CallbackTask(consumer: TransactionOperator, transactionState: TransactionState, callback: Callback) extends Runnable {

    override def toString() = s"CallbackTask($transactionState)"

    override def run() = {
      callback.onTransactionCall(consumer = consumer, partition = transactionState.partition, transactionID = transactionState.transactionID, count = transactionState.count)
    }
  }

}