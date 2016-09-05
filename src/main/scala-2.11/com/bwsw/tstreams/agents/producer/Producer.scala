package com.bwsw.tstreams.agents.producer

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.group.{CheckpointInfo, GroupParticipant, SendingAgent}
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy.ProducerPolicy
import com.bwsw.tstreams.common._
import com.bwsw.tstreams.coordination.client.BroadcastCommunicationClient
import com.bwsw.tstreams.coordination.messages.state.{TransactionStateMessage, TransactionStatus}
import com.bwsw.tstreams.coordination.producer.{AgentsStateDBService, PeerAgent}
import com.bwsw.tstreams.metadata.MetadataStorage
import com.bwsw.tstreams.streams.TStream
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
                         val stream: TStream[Array[Byte]],
                         val producerOptions: Options[T])
  extends GroupParticipant with SendingAgent with Interaction {

  /**
    * agent name
    */
  override def getAgentName = name

  def setAgentName(name: String) = {
    this.name = name
  }

  // shortkey
  val pcs = producerOptions.coordinationOptions
  var isStop = false

  private val openTransactions = new OpenTransactionsKeeper[T]()
  // stores latches for materialization await (protects from materialization before main transaction response)
  private val materializationGovernor = new MaterializationGovernor(producerOptions.writePolicy.getUsedPartitions().toSet)
  private val logger      = LoggerFactory.getLogger(this.getClass)
  private val threadLock  = new ReentrantLock(true)

  private val zkRetriesAmount = pcs.zkSessionTimeout * 1000 / PeerAgent.RETRY_SLEEP_TIME + 1
  private val zkService       = new ZookeeperDLMService(pcs.zkRootPath, pcs.zkHosts, pcs.zkSessionTimeout, pcs.zkConnectionTimeout)


  // amount of threads which will handle partitions in masters, etc
  val threadPoolSize: Int = {
    if (pcs.threadPoolAmount == -1)
      producerOptions.writePolicy.getUsedPartitions().size
    else
      pcs.threadPoolAmount
  }

  stream.dataStorage.bind() //TODO: fix, probably deprecated

  logger.info(s"Start new Basic producer with name : $name, streamName : ${stream.getName}, streamPartitions : ${stream.getPartitions}")

  private val agentsStateManager = new AgentsStateDBService(
    zkService,
    producerOptions.coordinationOptions.transport.getInetAddress(),
    stream.getName,
    Set[Int]().empty ++ producerOptions.writePolicy.getUsedPartitions())


  /**
    * P2P Agent for producers interaction
    * (getNewTxn uuid; publish openTxn event; publish closeTxn event)
    */
  override val p2pAgent: PeerAgent = new PeerAgent(
    agentsStateManager = agentsStateManager,
    zkService = zkService,
    zkRetriesAmount = zkRetriesAmount,
    producer = this,
    usedPartitions = producerOptions.writePolicy.getUsedPartitions(),
    isLowPriorityToBeMaster = pcs.isLowPriorityToBeMaster,
    transport = pcs.transport,
    poolSize = threadPoolSize)

  // this client is used to find new subscribers
  val subscriberNotifier = new BroadcastCommunicationClient(agentsStateManager, usedPartitions = producerOptions.writePolicy.getUsedPartitions())
  subscriberNotifier.init()

  /**
    * Queue to figure out moment when transaction is going to close
    */
  private val shutdownKeepAliveThread = new ThreadSignalSleepVar[Boolean](1)
  private val txnKeepAliveThread = getTxnKeepAliveThread
  val backendActivityService = new FirstFailLockableTaskExecutor(s"Producer ${name}-BackendWorker")
  val asyncActivityService = new FirstFailLockableTaskExecutor(s"Producer ${name}-AsyncWorker")
  val pendingCassandraTasks = new AtomicInteger(0)

  /**
    *
    */
  def getTxnKeepAliveThread: Thread = {
    val latch = new CountDownLatch(1)
    val txnKeepAliveThread = new Thread(new Runnable {
      override def run(): Unit = {
        Thread.currentThread().setName(s"Producer-${name}-KeepAlive")
        latch.countDown()
        logger.info(s"Producer ${name} - object is started, launched open transaction update thread")
        breakable {
          while (true) {
            val value: Boolean = shutdownKeepAliveThread.wait(producerOptions.transactionKeepAliveInterval * 1000)
            if (value) {
              logger.info(s"Producer ${name} - object either checkpointed or cancelled. Exit KeepAliveThread.")
              break()
            }
            asyncActivityService.submit(new Runnable {
              override def run(): Unit = updateOpenedTransactions()
            }, Option(threadLock))
          }
        }
      }
    })
    txnKeepAliveThread.start()
    latch.await()
    txnKeepAliveThread
  }

  /**
    * Used to send update event to all opened transactions
    */
  private def updateOpenedTransactions() = this.synchronized {
    logger.debug(s"Producer ${name} - scheduled for long lasting transactions")
    openTransactions.forallKeysDo((part: Int, txn: IProducerTransaction[T]) => txn.updateTxnKeepAliveState())
  }

  /**
    * @param policy        Policy for previous transaction on concrete partition
    * @param nextPartition Next partition to use for transaction (default -1 which mean that write policy will be used)
    * @return BasicProducerTransaction instance
    */
  def newTransaction(policy: ProducerPolicy, nextPartition: Int = -1): Transaction[T] = {
    if (isStop)
      throw new IllegalStateException(s"Producer ${this.name} is already stopped. Unable to get new transaction.")

    val partition = {
      if (nextPartition == -1)
        producerOptions.writePolicy.getNextPartition
      else
        nextPartition
    }

    if (!(partition >= 0 && partition < stream.getPartitions))
      throw new IllegalArgumentException(s"Producer ${name} - invalid partition")

    val previousTransactionAction: () => Unit =
      openTransactions.awaitOpenTransactionMaterialized(partition, policy)

    materializationGovernor.protect(partition)


    //val tm = getNewTxnUUIDLocal().timestamp()
    val txnUUID = p2pAgent.generateNewTransaction(partition)
    //val delta = txnUUID.timestamp()
    //logger.info(s"Elapsed for TXN ->: {}",delta - tm)
    if(logger.isDebugEnabled)
      logger.debug(s"[NEW_TRANSACTION PARTITION_$partition] uuid=${txnUUID.timestamp()}")
    val txn = new Transaction[T](partition, txnUUID, this)

    openTransactions.put(partition, txn)
    materializationGovernor.unprotect(partition)

    if(previousTransactionAction != null)
      asyncActivityService.submit(new Runnable {
        override def run(): Unit = previousTransactionAction() })
    txn
  }

  /**
    * Return transaction for specific partition if there is opened one.
    *
    * @param partition Partition from which transaction will be retrieved
    * @return Transaction reference if it exist and is opened
    */
  def getOpenedTransactionForPartition(partition: Int): Option[IProducerTransaction[T]] = {
    if (!(partition >= 0 && partition < stream.getPartitions))
      throw new IllegalArgumentException(s"Producer ${name} - invalid partition")
    openTransactions.getTransactionOption(partition)
  }

  /**
    * Checkpoint all opened transactions (not atomic). For atomic use CheckpointGroup.
    */
  def checkpoint(isAsynchronous: Boolean = false): Unit =
    openTransactions.forallKeysDo((k: Int, v: IProducerTransaction[T]) => v.checkpoint(isAsynchronous))


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
  def getNewTxnUUIDLocal(): UUID = {
    val transactionUuid = producerOptions.txnGenerator.getTimeUUID()
    transactionUuid
  }

  /**
    * Method to implement for concrete producer PeerAgent method
    * Need only if this producer is master
    *
    * @return UUID
    */
  override def openTxnLocal(txnUUID: UUID, partition: Int, onComplete: () => Unit): Unit = {
    stream.metadataStorage.commitEntity.commitAsync(
      streamName = stream.getName,
      partition = partition,
      transaction = txnUUID,
      totalCnt = -1,
      ttl = producerOptions.transactionTTL,
      resourceCounter = pendingCassandraTasks,
      function = () => {
        // submit task for materialize notification request
        p2pAgent.submitPipelinedTaskToMaterializeExecutor(partition, onComplete)
        // submit task for publish notification requests
        p2pAgent.submitPipelinedTaskToPublishExecutors(partition, () => {
            val msg = TransactionStateMessage(
              txnUuid = txnUUID,
              ttl = producerOptions.transactionTTL,
              status = TransactionStatus.opened,
              partition = partition,
              masterID = p2pAgent.getUniqueAgentID(),
              orderID = p2pAgent.getAndIncSequentialID(partition),
              count = 0)
            subscriberNotifier.publish(msg, () => ())
            if(logger.isDebugEnabled)
              logger.debug(s"Producer ${name} - [GET_LOCAL_TXN PRODUCER] update with msg partition=$partition uuid=${txnUUID.timestamp()} opened")
          })
      },
      executor = p2pAgent.getCassandraAsyncExecutor(partition))


  }


  /**
    * Stop this agent
    */
  def stop() = {
    logger.info(s"Producer ${name} is shutting down.")
    LockUtil.withLockOrDieDo[Unit](threadLock, (100, TimeUnit.SECONDS), Some(logger), () => {
      if (isStop)
        throw new IllegalStateException(s"Producer ${this.name} is already stopped. Duplicate action.")
      isStop = true })

    // stop update state of all open transactions
    shutdownKeepAliveThread.signal(true)
    txnKeepAliveThread.join()
    // stop executor

    asyncActivityService.shutdownOrDie(100, TimeUnit.SECONDS)
    while(pendingCassandraTasks.get() != 0) {
      logger.info(s"Waiting for all cassandra async callbacks will be executed. Pending: ${pendingCassandraTasks.get()}.")
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
  def materialize(msg: TransactionStateMessage) = {
    if(logger.isDebugEnabled)
      logger.debug(s"Start handling MaterializeRequest at partition: ${msg.partition}")
    materializationGovernor.awaitUnprotected(msg.partition)
    val opt = getOpenedTransactionForPartition(msg.partition)
    assert(opt.isDefined)
    if(logger.isDebugEnabled)
      logger.debug(s"In Map TXN: ${opt.get.getTransactionUUID.toString}\nIn Request TXN: ${msg.txnUuid}")
    assert(opt.get.getTransactionUUID == msg.txnUuid)
    assert(msg.status == TransactionStatus.materialize)
    opt.get.makeMaterialized()
    if(logger.isDebugEnabled)
      logger.debug("End handling MaterializeRequest")

  }
}