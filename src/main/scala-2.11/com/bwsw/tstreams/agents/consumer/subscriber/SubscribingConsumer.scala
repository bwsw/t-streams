package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.UUID
import java.util.concurrent.{ExecutorService, Executors}

import com.bwsw.tstreams.agents.consumer.{Consumer, Options, Transaction, SubscriberCoordinationOptions}
import com.bwsw.tstreams.coordination.subscriber.Coordinator
import com.bwsw.tstreams.streams.TStream
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
  * @tparam USERTYPE User data type
  */
class SubscribingConsumer[USERTYPE](name: String,
                                    stream: TStream[Array[Byte]],
                                    options: Options[USERTYPE],
                                    subscriberCoordinationOptions: SubscriberCoordinationOptions,
                                    callBack: Callback[USERTYPE],
                                    persistentQueuePath: String,
                                    pollingFrequencyMaxDelay: Int = 100)
  extends Consumer[USERTYPE](name, stream, options) {

  /**
    * agent name
    */
  override def getAgentName = name

  /**
    * Indicate started or not this subscriber
    */
  //private var isStarted = false
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Coordinator for providing updates to this subscriber from producers
    * and establishing stream locks
    */
  private var coordinator = new Coordinator(
    subscriberCoordinationOptions.agentAddress,
    subscriberCoordinationOptions.zkRootPath,
    subscriberCoordinationOptions.zkHosts,
    subscriberCoordinationOptions.zkSessionTimeout,
    subscriberCoordinationOptions.zkConnectionTimeout)

  /**
    * Subscriber used partitions
    */
  private val usedPartitions = options.readPolicy.getUsedPartitions()

  /**
    * Thread pool size (default is equal to [[usedPartitions.size]]])
    */
  private val poolSize = if (subscriberCoordinationOptions.threadPoolAmount == -1)
    usedPartitions.size
  else
    subscriberCoordinationOptions.threadPoolAmount

  logger.info("Will start " + poolSize + " executors to serve master and asynchronous activity.")
  logger.info("Will do out of band polling every " + pollingFrequencyMaxDelay + "ms.")

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
  private var updateManager: UpdateManager = null

  /**
    * Resolver for resolving pre/final commit's
    */
  private var brokenTransactionsResolver: BrokenTransactionsResolver = null

  /**
    * Start subscriber to consume new transactions
    */
  override def start(): Unit = {
    if (isStarted.get())
      throw new IllegalStateException("Subscriber already started")

    getThreadLock().lock()
    super.start()

    updateManager = new UpdateManager
    brokenTransactionsResolver =  new BrokenTransactionsResolver(this)

    // TODO: why it can be stopped, why reconstruct?
    if (coordinator.isStoped) {
      coordinator = new Coordinator(
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
    updateManager.startUpdate(pollingFrequencyMaxDelay)
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
    getThreadLock().unlock()
    isStarted.set(true)
  }

  def resolveLastTxn(partition: Int): Option[Transaction[USERTYPE]] = {
    val txn: Option[Transaction[USERTYPE]] = getLastTransaction(partition)
    txn.fold[Option[Transaction[USERTYPE]]](None) { txn =>
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
  override def stop() = {
    if (!isStarted.get())
      throw new IllegalStateException("Subscriber is not started")
    updateManager.stopUpdate()
    if (executors != null) {
      executors.foreach(x => x._2.shutdown())
      executors.clear()
    }
    coordinator.stop()
    brokenTransactionsResolver.stop()
    isStarted.set(false)
  }
}
