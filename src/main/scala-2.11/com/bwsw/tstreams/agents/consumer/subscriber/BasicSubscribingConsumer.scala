package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.UUID
import java.util.concurrent.{ExecutorService, Executors}

import com.bwsw.tstreams.agents.consumer.{BasicConsumer, BasicConsumerOptions, BasicConsumerTransaction, SubscriberCoordinationOptions}
import com.bwsw.tstreams.coordination.pubsub.SubscriberCoordinator
import com.bwsw.tstreams.streams.BasicStream
import com.bwsw.tstreams.txnqueue.PersistentTransactionQueue
import org.slf4j.LoggerFactory

/**
  * Basic consumer with subscribe option
  *
  * @param name                Name of subscriber
  * @param stream              Stream from which to consume transactions
  * @param options             Basic consumer options
  * @param persistentQueuePath Local path for queue which maintain transactions that already exist
  *                            and new incoming transactions
  * @tparam DATATYPE Storage data type
  * @tparam USERTYPE User data type
  */
class BasicSubscribingConsumer[DATATYPE, USERTYPE](name: String,
                                                   stream: BasicStream[DATATYPE],
                                                   options: BasicConsumerOptions[DATATYPE, USERTYPE],
                                                   subscriberCoordinationOptions: SubscriberCoordinationOptions,
                                                   callBack: BasicSubscriberCallback[DATATYPE, USERTYPE],
                                                   persistentQueuePath: String)
  extends BasicConsumer[DATATYPE, USERTYPE](name, stream, options) {
  /**
    * Indicate started or not this subscriber
    */
  private var isStarted = false
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Coordinator for providing updates to this subscriber from producers
    * and establishing stream locks
    */
  private var coordinator = new SubscriberCoordinator(
    subscriberCoordinationOptions.agentAddress,
    subscriberCoordinationOptions.zkRootPath,
    subscriberCoordinationOptions.zkHosts,
    subscriberCoordinationOptions.zkSessionTimeout,
    subscriberCoordinationOptions.zkConnectionTimeout)

  /**
    * Subscriber used partitions
    */
  private val usedPartitions = options.readPolicy.getUsedPartition()

  /**
    * Thread pool size (default is equal to [[usedPartitions.size]]])
    */
  private val poolSize = if (subscriberCoordinationOptions.threadPoolAmount == -1)
    usedPartitions.size
  else
    subscriberCoordinationOptions.threadPoolAmount

  logger.info("Will start " + poolSize + " executors to serve master and asynchronous activity.")

  /**
    * Mapping partitions to executors index
    */
  private val partitionsToExecutors = usedPartitions
    .zipWithIndex
    .map { case (partition, execNum) => (partition, execNum % poolSize) }
    .toMap

  /**
    * Executors for each partition to handle transactions flow
    */
  private val executors: scala.collection.mutable.Map[Int, ExecutorService] =
    scala.collection.mutable.Map[Int, ExecutorService]()

  /**
    * Manager for providing updates on transactions
    */
  private val updateManager = new UpdateManager

  /**
    * Resolver for resolving pre/final commit's
    */
  private val brokenTransactionsResolver = new BrokenTransactionsResolver(this)

  /**
    * Start subscriber to consume new transactions
    */
  def start() = {
    if (isStarted)
      throw new IllegalStateException("subscriber already started")
    isStarted = true

    // TODO: why it can be stopped, why reconstruct?
    if (coordinator.isStoped) {
      coordinator = new SubscriberCoordinator(
        subscriberCoordinationOptions.agentAddress,
        subscriberCoordinationOptions.zkRootPath,
        subscriberCoordinationOptions.zkHosts,
        subscriberCoordinationOptions.zkSessionTimeout,
        subscriberCoordinationOptions.zkConnectionTimeout)
    }

    brokenTransactionsResolver.startUpdate()

    val streamLock = coordinator.getStreamLock(stream.getName)
    streamLock.lock()

    (0 until poolSize) foreach { x =>
      executors(x) = Executors.newSingleThreadExecutor()
    }

    coordinator.initSynchronization(stream.getName, usedPartitions)
    coordinator.startListen()
    updateManager.startUpdate(callBack.pollingFrequency)
    val uniquePrefix = UUID.randomUUID()

    usedPartitions foreach { partition =>
      val lastTransactionOpt = resolveLastTxn(partition)
      val queue =
        if (lastTransactionOpt.isDefined) {
          val txnUuid = lastTransactionOpt.get.getTxnUUID
          new PersistentTransactionQueue(persistentQueuePath + s"/$uniquePrefix/$partition", txnUuid) // TODO: fix with correct FS concat
        }
        else {
          new PersistentTransactionQueue(persistentQueuePath + s"/$uniquePrefix/$partition", null) // TODO: fix with correct FS concat
        }

      val lastTxnUuid = if (lastTransactionOpt.isDefined) {
        lastTransactionOpt.get.getTxnUUID
      }
      else
        currentOffsets(partition)

      val executorIndex = partitionsToExecutors(partition)
      val executor = executors(executorIndex)
      val lastTxn = new LastTransactionWrapper(lastTxnUuid)

      val transactionsRelay = new SubscriberTransactionsRelay(
        subscriber = this,
        partition = partition,
        coordinator = coordinator,
        callback = callBack,
        queue = queue,
        lastConsumedTransaction = lastTxn,
        executor = executor,
        checkpointEventsResolver = brokenTransactionsResolver)

      //consume all transactions less or equal than last transaction
      if (lastTransactionOpt.isDefined) {
        transactionsRelay.consumeTransactionsLessOrEqualThan(
          leftBorder = currentOffsets(partition),
          rightBorder = lastTransactionOpt.get.getTxnUUID)
      } else {
        //TODO: what behaviour
      }

      transactionsRelay.notifyProducersAndStartListen()

      //consume all transactions strictly greater than last
      if (lastTransactionOpt.isDefined) {
        transactionsRelay.consumeTransactionsMoreThan(leftBorder = lastTransactionOpt.get.getTxnUUID)
      } else {
        transactionsRelay.consumeTransactionsMoreThan(leftBorder = currentOffsets(partition))
      }

      updateManager.addExecutorWithRunnable(executor, transactionsRelay.getUpdateRunnable())
    }

    streamLock.unlock()
  }

  def resolveLastTxn(partition: Int): Option[BasicConsumerTransaction[DATATYPE, USERTYPE]] = {
    val txn: Option[BasicConsumerTransaction[DATATYPE, USERTYPE]] = getLastTransaction(partition)
    txn.fold[Option[BasicConsumerTransaction[DATATYPE, USERTYPE]]](None) { txn =>
      if (txn.getTxnUUID.timestamp() <= currentOffsets(partition).timestamp()) {
        None
      } else {
        Some(txn)
      }
    }
  }

  /**
    * Stop subscriber
    */
  def stop() = {
    if (!isStarted)
      throw new IllegalStateException("subscriber is not started")
    isStarted = false
    updateManager.stopUpdate()
    if (executors != null) {
      executors.foreach(x => x._2.shutdown())
      executors.clear()
    }
    coordinator.stop()
    brokenTransactionsResolver.stop()
  }
}
