package com.bwsw.tstreams.agents.producer

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.group.{CheckpointInfo, GroupParticipant, SendingAgent}
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy.ProducerPolicy
import com.bwsw.tstreams.common._
import com.bwsw.tstreams.coordination.client.BroadcastCommunicationClient
import com.bwsw.tstreams.coordination.messages.state.{TransactionStateMessage, TransactionStatus}
import com.bwsw.tstreams.coordination.producer.{AgentsStateDBService, PeerAgent}
import com.bwsw.tstreams.metadata.{TransactionRecord, TransactionDatabase, MetadataStorage}
import com.bwsw.tstreams.streams.Stream
import org.slf4j.LoggerFactory

import scala.util.control.Breaks._


/**
  * Basic producer class
  *
  * @param name            Producer name
  * @param stream          Stream for transaction sending
  * @param producerOptions This producer options
  * @tparam T User data type
  */
class Producer[T](var name: String,
                  val stream: Stream[Array[Byte]],
                  val producerOptions: ProducerOptions[T])
  extends GroupParticipant with SendingAgent with Interaction {

  /**
    * agent name
    */
  override def getAgentName() = name

  def setAgentName(name: String) = {
    this.name = name
  }

  val tsdb = new TransactionDatabase(stream.getMetadataStorage().getSession(), stream.getName())

  /**
    * Allows to get if the producer is master for the partition.
    *
    * @param partition
    * @return
    */
  def isMeAMasterOfPartition(partition: Int): Boolean = {
    val masterOpt = agentsStateManager.getCurrentMaster(partition)
    masterOpt.fold(false) { m => producerOptions.coordinationOptions.transport.getInetAddress() == m.agentAddress }
  }

  def getLocalPartitionMasterID(partition: Int): Int = {
    val masterOpt = agentsStateManager.getCurrentMasterLocal(partition)
    masterOpt.fold(0) { m => m.uniqueAgentId }
  }

  def isLocalMePartitionMaster(partition: Int): Boolean = {
    val masterOpt = agentsStateManager.getCurrentMasterLocal(partition)
    masterOpt.fold(false) { m => producerOptions.coordinationOptions.transport.getInetAddress() == m.agentAddress }
  }

  def dumpPartitionsOwnership() = agentsStateManager.dumpPartitionsOwnership()

  /**
    * Utility method which allows waiting while the producer completes partition redistribution process.
    * Used mainly in integration tests.
    */
  def awaitPartitionRedistributionThreadComplete() = p2pAgent.awaitPartitionRedistributionThreadComplete.await()

  // short key
  val pcs = producerOptions.coordinationOptions
  var isStop = false

  private val openTransactions = new OpenTransactionsKeeper[T]()
  // stores latches for materialization await (protects from materialization before main transaction response)
  private val materializationGovernor = new MaterializationGovernor(producerOptions.writePolicy.getUsedPartitions().toSet)
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val threadLock = new ReentrantLock(true)

  private val peerKeepAliveTimeout = pcs.zkSessionTimeout * 1000 + 2000 // TODO: fix it!
  private val zkService = new ZookeeperDLMService(pcs.zkRootPath, pcs.zkHosts, pcs.zkSessionTimeout, pcs.zkConnectionTimeout)


  // amount of threads which will handle partitions in masters, etc
  val threadPoolSize: Int = {
    if (pcs.threadPoolAmount == -1)
      producerOptions.writePolicy.getUsedPartitions().size
    else
      pcs.threadPoolAmount
  }

  stream.dataStorage.bind()

  logger.info(s"Start new Basic producer with name : $name, streamName : ${stream.getName}, streamPartitions : ${stream.getPartitions}")

  private val agentsStateManager = new AgentsStateDBService(
    zkService,
    producerOptions.coordinationOptions.transport.getInetAddress(),
    stream.getName,
    Set[Int]().empty ++ producerOptions.writePolicy.getUsedPartitions())


  /**
    * P2P Agent for producers interaction
    * (getNewTransaction id; publish openTransaction event; publish closeTransaction event)
    */
  override val p2pAgent: PeerAgent = new PeerAgent(
    agentsStateManager = agentsStateManager,
    zkService = zkService,
    peerKeepAliveTimeout = peerKeepAliveTimeout,
    producer = this,
    usedPartitions = producerOptions.writePolicy.getUsedPartitions(),
    isLowPriorityToBeMaster = pcs.isLowPriorityToBeMaster,
    transport = pcs.transport,
    threadPoolAmount = threadPoolSize,
    threadPoolPublisherThreadsAmount = pcs.threadPoolPublisherThreadsAmount,
    partitionRedistributionDelay = pcs.partitionRedistributionDelay,
    isMasterBootstrapModeFull = pcs.isMasterBootstrapModeFull,
    isMasterProcessVote = pcs.isMasterProcessVote)

  // this client is used to find new subscribers
  val subscriberNotifier = new BroadcastCommunicationClient(agentsStateManager, usedPartitions = producerOptions.writePolicy.getUsedPartitions())
  subscriberNotifier.init()

  /**
    * Queue to figure out moment when transaction is going to close
    */
  private val shutdownKeepAliveThread = new ThreadSignalSleepVar[Boolean](1)
  private val transactionKeepAliveThread = getTransactionKeepAliveThread
  val backendActivityService = new FirstFailLockableTaskExecutor(s"Producer $name-BackendWorker")
  val asyncActivityService = new FirstFailLockableTaskExecutor(s"Producer $name-AsyncWorker")

  /**
    *
    */
  def getTransactionKeepAliveThread: Thread = {
    val latch = new CountDownLatch(1)
    val transactionKeepAliveThread = new Thread(new Runnable {
      override def run(): Unit = {
        Thread.currentThread().setName(s"Producer-$name-KeepAlive")
        latch.countDown()
        logger.info(s"Producer $name - object is started, launched open transaction update thread")
        breakable {
          while (true) {
            val value: Boolean = shutdownKeepAliveThread.wait(producerOptions.transactionKeepAliveInterval * 1000)
            if (value) {
              logger.info(s"Producer $name - object either checkpointed or cancelled. Exit KeepAliveThread.")
              break()
            }
            asyncActivityService.submit("<UpdateOpenedTransactionsTask>", new Runnable {
              override def run(): Unit = updateOpenedTransactions()
            }, Option(threadLock))
          }
        }
      }
    })
    transactionKeepAliveThread.start()
    latch.await()
    transactionKeepAliveThread
  }

  /**
    * Used to send update event to all opened transactions
    */
  private def updateOpenedTransactions() = this.synchronized {
    logger.debug(s"Producer $name - scheduled for long lasting transactions")
    openTransactions.forallKeysDo((part: Int, transaction: IProducerTransaction[T]) => transaction.updateTransactionKeepAliveState())
  }

  private def newTransactionUnsafe(policy: ProducerPolicy, partition: Int = -1): ProducerTransaction[T] = {
    if (isStop)
      throw new IllegalStateException(s"Producer ${this.name} is already stopped. Unable to get new transaction.")

    val p = {
      if (partition == -1)
        producerOptions.writePolicy.getNextPartition
      else
        partition
    }

    if (!(p >= 0 && p < stream.getPartitions))
      throw new IllegalArgumentException(s"Producer $name - invalid partition")

    val previousTransactionAction: () => Unit =
      openTransactions.awaitOpenTransactionMaterialized(p, policy)

    materializationGovernor.protect(p)


    val transactionID = p2pAgent.generateNewTransaction(p)
    if (logger.isDebugEnabled)
      logger.debug(s"[NEW_TRANSACTION PARTITION_$p] ID=$transactionID")
    val transaction = new ProducerTransaction[T](p, transactionID, this)

    openTransactions.put(p, transaction)
    materializationGovernor.unprotect(p)

    if (previousTransactionAction != null)
      asyncActivityService.submit("<PreviousTransactionActionTask>", new Runnable {
        override def run(): Unit = previousTransactionAction()
      })
    transaction
  }

  /**
    * @param policy    Policy for previous transaction on concrete partition
    * @param partition Next partition to use for transaction (default -1 which mean that write policy will be used)
    * @return BasicProducerTransaction instance
    */
  def newTransaction(policy: ProducerPolicy, partition: Int = -1, retry: Int = 1): ProducerTransaction[T] = {
    if (isStop)
      throw new IllegalStateException(s"Producer ${this.name} is already stopped. Unable to get new transaction.")

    if(retry < 0)
      throw new IllegalStateException("Failed to get a new transaction.")

    try {
      val transaction = newTransactionUnsafe(policy, partition)
      transaction.awaitMaterialized()
      transaction
    } catch {
      case e: MaterializationException =>
        newTransaction(policy, retry - 1)
    }
  }


  /**
    * Return transaction for specific partition if there is opened one.
    *
    * @param partition Partition from which transaction will be retrieved
    * @return Transaction reference if it exist and is opened
    */
  def getOpenedTransactionForPartition(partition: Int): Option[IProducerTransaction[T]] = {
    if (!(partition >= 0 && partition < stream.getPartitions))
      throw new IllegalArgumentException(s"Producer $name - invalid partition")
    openTransactions.getTransactionOption(partition)
  }

  /**
    * Checkpoint all opened transactions (not atomic). For atomic use CheckpointGroup.
    */
  def checkpoint(isSynchronous: Boolean = true): Unit =
    openTransactions.forallKeysDo((k: Int, v: IProducerTransaction[T]) => v.checkpoint(isSynchronous))


  /**
    * Cancel all opened transactions (not atomic). For atomic use CheckpointGroup.
    */
  def cancel(): Unit =
    openTransactions.forallKeysDo((k: Int, v: IProducerTransaction[T]) => v.cancel())


  /**
    * Finalize all opened transactions (not atomic). For atomic use CheckpointGroup.
    */
  def finalizeDataSend(): Unit = {
    openTransactions.forallKeysDo((k: Int, v: IProducerTransaction[T]) => v.finalizeDataSend())
  }

  /**
    * Info to commit
    */
  override def getCheckpointInfoAndClear(): List[CheckpointInfo] = {
    val checkpointInfo = openTransactions.forallKeysDo((k: Int, v: IProducerTransaction[T]) => v.getTransactionInfo()).toList
    openTransactions.clear()
    checkpointInfo
  }

  /**
    * @return Metadata storage link for concrete agent
    */
  override def getMetadataRef(): MetadataStorage =
    stream.metadataStorage

  /**
    *
    * @return
    */
  def getNewTransactionIDLocal() =
    producerOptions.transactionGenerator.getTransaction()

  /**
    * Method to implement for concrete producer PeerAgent method
    * Need only if this producer is master
    *
    * @return ID
    */
  override def openTransactionLocal(transactionID: Long, partition: Int, onComplete: () => Unit): Unit = {

    p2pAgent.submitPipelinedTaskToPublishExecutors(partition, () => {
      val msg = TransactionStateMessage(
        transactionID = transactionID,
        ttl = producerOptions.transactionTTL,
        status = TransactionStatus.opened,
        partition = partition,
        masterID = p2pAgent.getUniqueAgentID(),
        orderID = p2pAgent.getAndIncSequentialID(partition),
        count = 0)
      subscriberNotifier.publish(msg, () => ())
      if (logger.isDebugEnabled)
        logger.debug(s"Producer $name - [GET_LOCAL_TRANSACTION] update with message partition=$partition ID=$transactionID opened")
    })

    val transactionRecord = TransactionRecord(partition = partition, transactionID = transactionID, count = -1,
      ttl = producerOptions.transactionTTL)

    tsdb.put(transaction = transactionRecord, asynchronousExecutor = p2pAgent.getCassandraAsyncExecutor(partition)) (rec => {
      p2pAgent.submitPipelinedTaskToMaterializeExecutor(partition, onComplete)
    })

  }


  /**
    * Stop this agent
    */
  def stop() = {
    logger.info(s"Producer $name is shutting down.")
    LockUtil.withLockOrDieDo[Unit](threadLock, (100, TimeUnit.SECONDS), Some(logger), () => {
      if (isStop)
        throw new IllegalStateException(s"Producer ${this.name} is already stopped. Duplicate action.")
      isStop = true
    })

    // stop update state of all open transactions
    shutdownKeepAliveThread.signal(true)
    transactionKeepAliveThread.join()
    // stop executor

    asyncActivityService.shutdownOrDie(100, TimeUnit.SECONDS)
    while (tsdb.getResourceCounter() != 0) {
      logger.info(s"Waiting for all cassandra async callbacks will be executed. Pending: ${tsdb.getResourceCounter()}.")
      Thread.sleep(200)
    }
    backendActivityService.shutdownOrDie(100, TimeUnit.SECONDS)

    // stop provide master features to public
    p2pAgent.stop()
    // stop function which works with subscribers
    subscriberNotifier.stop()
    zkService.close()
  }

  /**
    * Agent lock on any actions which has to do with checkpoint
    */
  override def getThreadLock(): ReentrantLock = threadLock


  /**
    * Special method which waits until newTransaction method will be completed and after
    * does materialization. It's called from another thread (p2pAgent), not from thread of
    * Producer.
    *
    * @param msg
    */
  def materialize(msg: TransactionStateMessage):Unit = {

    if (logger.isDebugEnabled)
      logger.debug(s"Start handling MaterializeRequest at partition: ${msg.partition}")

    materializationGovernor.awaitUnprotected(msg.partition)
    val opt = getOpenedTransactionForPartition(msg.partition)

    if(opt.isEmpty) {
      logger.warn(s"There is no opened transaction for ${msg.partition}.")
      return
    }

    if (logger.isDebugEnabled)
      logger.debug(s"In Map Transaction: ${opt.get.getTransactionID.toString}\nIn Request Transaction: ${msg.transactionID}")

    if(!(opt.get.getTransactionID == msg.transactionID && msg.status == TransactionStatus.materialize)) {
      logger.warn(s"Materialization is requested for transaction ${msg.transactionID} but expected transaction is ${opt.get.getTransactionID}.")
      opt.get.markAsClosed()
      return
    }

    opt.get.makeMaterialized()
    if (logger.isDebugEnabled)
      logger.debug("End handling MaterializeRequest")

  }
}