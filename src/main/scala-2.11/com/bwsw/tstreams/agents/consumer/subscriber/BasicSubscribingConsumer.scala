package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.concurrent.{Executors, ExecutorService}

import com.bwsw.tstreams.agents.consumer.{BasicConsumer, BasicConsumerOptions}
import com.bwsw.tstreams.streams.BasicStream
import com.bwsw.tstreams.txnqueue.PersistentTransactionQueue

/**
 * Basic consumer with subscribe option
 * @param name Name of consumer
 * @param stream Stream from which to consume transactions
 * @param options Basic consumer options
 * @param persistentQueuePath Local Path to queue which maintain transactions that already exist and new incoming transactions
 * @tparam DATATYPE Storage data type
 * @tparam USERTYPE User data type
 */
class BasicSubscribingConsumer[DATATYPE, USERTYPE](name : String,
                                                   stream : BasicStream[DATATYPE],
                                                   options : BasicConsumerOptions[DATATYPE,USERTYPE],
                                                   callBack : BasicSubscriberCallback[DATATYPE, USERTYPE],
                                                   persistentQueuePath : String)
  extends BasicConsumer[DATATYPE, USERTYPE](name, stream, options){
  private var cntRun = 0
  private var isStarted = false
  private val usedPartitions = options.readPolicy.getUsedPartition()
  private val poolSize = if (options.consumerCoordinatorSettings.threadPoolAmount == -1)
    usedPartitions.size
  else
    options.consumerCoordinatorSettings.threadPoolAmount
  private val partitionsToExecutors = usedPartitions
    .zipWithIndex
    .map{case(partition,execNum) => (partition,execNum % poolSize)}
    .toMap
  private var executors : scala.collection.mutable.Map[Int, ExecutorService] = null
  private val updateManager = new UpdateManager

  /**
   * Start subscriber consuming new transactions
   */
  def start() = {
    if (isStarted)
      throw new IllegalStateException("subscriber already started")
    isStarted = true
    cntRun += 1

    executors = scala.collection.mutable.Map[Int, ExecutorService]()
    (0 until poolSize) foreach { x =>
      executors(x) = Executors.newSingleThreadExecutor()
    }

    coordinator.startListen()
    coordinator.startCallback()
    updateManager.startUpdate(callBack.frequency)

    (0 until stream.getPartitions) foreach { partition =>
      val lastTransactionOpt = getLastTransaction(partition)

      val queue =
        if (lastTransactionOpt.isDefined) {
          val txnUuid = lastTransactionOpt.get.getTxnUUID
          new PersistentTransactionQueue(persistentQueuePath + s"/$cntRun/$partition", txnUuid)
        }
        else {
          new PersistentTransactionQueue(persistentQueuePath + s"/$cntRun/$partition", null)
        }

      val lastTxnUuid = if (lastTransactionOpt.isDefined)
        lastTransactionOpt.get.getTxnUUID
      else
        options.txnGenerator.getTimeUUID(0)

      val executorIndex = partitionsToExecutors(partition)
      val executor = executors(executorIndex)

      val transactionsRelay = new SubscriberTransactionsRelay(
        subscriber = this,
        offset = currentOffsets(partition),
        partition = partition,
        coordinator = coordinator,
        callback = callBack,
        queue = queue,
        lastTransaction = lastTxnUuid,
        executor = executor)

      //consume all transactions less or equal than last transaction
      if (lastTransactionOpt.isDefined)
        transactionsRelay.consumeTransactionsLessOrEqualThan(lastTransactionOpt.get.getTxnUUID)

      transactionsRelay.notifyProducersAndStartListen()

      //consume all transactions strictly greater than last
      if (lastTransactionOpt.isDefined)
        transactionsRelay.consumeTransactionsMoreThan(lastTransactionOpt.get.getTxnUUID)
      else {
        val oldestUuid = options.txnGenerator.getTimeUUID(0)
        transactionsRelay.consumeTransactionsMoreThan(oldestUuid)
      }

      updateManager.addExecutorWithRunnable(executor,transactionsRelay.getUpdateRunnable())
    }
  }

  /**
   *
   */
  override def stop() = {
    if (!isStarted)
      throw new IllegalStateException("subscriber is not started")
    if (executors != null)
      executors.foreach(x=>x._2.shutdown())
    updateManager.stopUpdate()
    isStarted = false
    coordinator.stop()
  }
}
