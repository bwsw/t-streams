package com.bwsw.tstreams.agents.producer

import java.util.UUID
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{TimeUnit, CountDownLatch}

import com.bwsw.tstreams.agents.group.{Agent, CheckpointInfo, ProducerCheckpointInfo}
import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy.ProducerPolicy
import com.bwsw.tstreams.common.{LockUtil, FirstFailLockableTaskExecutor, ThreadSignalSleepVar}
import com.bwsw.tstreams.coordination.pubsub.ProducerToSubscriberNotifier
import com.bwsw.tstreams.coordination.pubsub.messages.{ProducerTopicMessage, ProducerTransactionStatus}
import com.bwsw.tstreams.coordination.transactions.peertopeer.PeerToPeerAgent
import com.bwsw.tstreams.coordination.transactions.transport.traits.Interaction
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
  * @tparam USERTYPE User data type
  */
class Producer[USERTYPE](val name: String,
                         val stream: TStream[Array[Byte]],
                         val producerOptions: Options[USERTYPE])
  extends Agent with Interaction {

  /**
    * agent name
    */
  override def getAgentName = name

  // shortkey
  val pcs = producerOptions.coordinationOptions

  var isStop = false

  // stores currently opened transactions per partition
  private val openTransactionsMap = scala.collection.mutable.Map[Int, Transaction[USERTYPE]]()
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val threadLock = new ReentrantLock(true)

  // amount of threads which will handle partitions in masters, etc
  val threadPoolSize: Int = {
    if (pcs.threadPoolAmount == -1)
      producerOptions.writePolicy.getUsedPartitions().size
    else
      pcs.threadPoolAmount
  }

  val txnLocks = new Array[ReentrantLock](threadPoolSize)

  (0 until threadPoolSize) foreach { idx =>
    txnLocks(idx) = new ReentrantLock()
  }

  stream.dataStorage.bind() //TODO: fix, probably deprecated

  logger.info(s"Start new Basic producer with name : $name, streamName : ${stream.getName}, streamPartitions : ${stream.getPartitions}")

  // this client is used to find new subscribers
  val subscriberNotifier = new ProducerToSubscriberNotifier(
    prefix = pcs.zkRootPath,
    streamName = stream.getName,
    usedPartitions = producerOptions.writePolicy.getUsedPartitions(),
    zkHosts = pcs.zkHosts,
    zkSessionTimeout = pcs.zkSessionTimeout,
    zkConnectionTimeout = pcs.zkConnectionTimeout)

  /**
    * P2P Agent for producers interaction
    * (getNewTxn uuid; publish openTxn event; publish closeTxn event)
    */
  override val masterP2PAgent: PeerToPeerAgent = new PeerToPeerAgent(
    agentAddress = pcs.agentAddress,
    zkHosts = pcs.zkHosts,
    zkRootPath = pcs.zkRootPath,
    zkSessionTimeout = pcs.zkSessionTimeout,
    zkConnectionTimeout = pcs.zkConnectionTimeout,
    producer = this,
    usedPartitions = producerOptions.writePolicy.getUsedPartitions(),
    isLowPriorityToBeMaster = pcs.isLowPriorityToBeMaster,
    transport = pcs.transport,
    transportTimeout = pcs.transportTimeout,
    poolSize = threadPoolSize)

  //used for managing new agents on stream

  {
    val zkStreamLock = subscriberNotifier.getStreamLock(stream.getName)
    zkStreamLock.lock()
    subscriberNotifier.init()
    zkStreamLock.unlock()
  }


  /**
    * Queue to figure out moment when transaction is going to close
    */
  private val shutdownKeepAliveThread = new ThreadSignalSleepVar[Boolean](10)
  private val txnKeepAliveThread = getTxnKeepAliveThread
  val backendActivityService = new FirstFailLockableTaskExecutor

  /**
    *
    */
  def getTxnKeepAliveThread: Thread = {
    val latch = new CountDownLatch(1)
    val txnKeepAliveThread = new Thread(new Runnable {
      override def run(): Unit = {
        latch.countDown()
        logger.info(s"Producer ${name} - object is started, launched open transaction update thread")
        breakable {
          while (true) {
            val value: Boolean = shutdownKeepAliveThread.wait(producerOptions.transactionKeepAliveInterval * 1000)
            if (value) {
              logger.info(s"Producer ${name} - object either checkpointed or cancelled. Exit KeepAliveThread.")
              break()
            }
            backendActivityService.submit(new Runnable {
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
    *
    */
  def updateOpenedTransactions() = {
    logger.debug(s"Producer ${name} - scheduled for long lasting transactions")
    openTransactionsMap.
      map { case (partition, txn) => txn }.
      foreach { x => if (!x.isClosed) x.updateTxnKeepAliveState() }
  }

  /**
    * @param policy        Policy for previous transaction on concrete partition
    * @param nextPartition Next partition to use for transaction (default -1 which mean that write policy will be used)
    * @return BasicProducerTransaction instance
    */
  def newTransaction(policy: ProducerPolicy, nextPartition: Int = -1): Transaction[USERTYPE] = {
    LockUtil.withLockOrDieDo[Transaction[USERTYPE]] (threadLock, (100, TimeUnit.SECONDS), Some(logger), () => {
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

      val transaction = {
        val txnUUID = masterP2PAgent.getNewTxn(partition)
        logger.debug(s"[NEW_TRANSACTION PARTITION_$partition] uuid=${txnUUID.timestamp()}")
        if (openTransactionsMap.contains(partition)) {
          val prevTxn = openTransactionsMap(partition)
          if (!prevTxn.isClosed) {
            policy match {
              case NewTransactionProducerPolicy.CheckpointIfOpened =>
                prevTxn.checkpoint()

              case NewTransactionProducerPolicy.CancelIfOpened =>
                prevTxn.cancel()

              case NewTransactionProducerPolicy.ErrorIfOpened =>
                throw new IllegalStateException(s"Producer ${name} - previous transaction was not closed")
            }
          }
        }
        val txn = new Transaction[USERTYPE](txnLocks(partition % threadPoolSize), partition, txnUUID, this)
        openTransactionsMap(partition) = txn
        txn
      }
      transaction })
  }

  /**
    * Return reference on transaction from concrete partition
    *
    * @param partition Partition from which transaction will be retrieved
    * @return Transaction reference if it exist or not closed
    */
  def getOpenTransactionForPartition(partition: Int): Option[Transaction[USERTYPE]] = {
    LockUtil.withLockOrDieDo[Option[Transaction[USERTYPE]]](threadLock, (100, TimeUnit.SECONDS), Some(logger), () => {
      if (!(partition >= 0 && partition < stream.getPartitions))
        throw new IllegalArgumentException(s"Producer ${name} - invalid partition")

      val res = if (openTransactionsMap.contains(partition)) {
        val txn = openTransactionsMap(partition)
        if (txn.isClosed)
          return None
        Some(txn)
      }
      else
        None
      res })
  }

  /**
    * Close all opened transactions
    */
  def checkpoint(): Unit = {
    LockUtil.withLockOrDieDo[Unit](threadLock, (100, TimeUnit.SECONDS), Some(logger), () => {
      openTransactionsMap.
        map { case (partition, txn) => txn }.
        foreach { x => if (!x.isClosed) x.checkpoint() }})
  }

  /**
    * Info to commit
    */
  override def getCheckpointInfoAndClear(): List[CheckpointInfo] = {
    val checkpointData = openTransactionsMap.filter(k => !k._2.isClosed).map {
      case (partition, txn) =>

        val (txnUuid, txnCnt, txnPartition) =
          LockUtil.withLockOrDieDo[(UUID,Int,Int)](txn.getTransactionLock, (100, TimeUnit.SECONDS), Some(logger), () => {
          val txnUuid = txn.getTxnUUID
          val txnCnt = txn.getCnt
          val txnPartition = txn.getPartition
          txn.setAsClosed()
          (txnUuid, txnCnt, txnPartition)
        })

        assert(partition == txnPartition)

        val preCheckpoint = ProducerTopicMessage(
          txnUuid = txnUuid,
          ttl = -1,
          status = ProducerTransactionStatus.preCheckpoint,
          partition = partition)

        val finalCheckpoint = ProducerTopicMessage(
          txnUuid = txnUuid,
          ttl = -1,
          status = ProducerTransactionStatus.postCheckpoint,
          partition = partition)

        ProducerCheckpointInfo(transactionRef = txn,
          agent = masterP2PAgent,
          preCheckpointEvent = preCheckpoint,
          finalCheckpointEvent = finalCheckpoint,
          streamName = stream.getName,
          partition = partition,
          transaction = txnUuid,
          totalCnt = txnCnt,
          ttl = stream.getTTL)
    }.toList

    openTransactionsMap.clear()
    checkpointData
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
    * Method to implement for concrete producer [[PeerToPeerAgent]] method
    * Need only if this producer is master
    *
    * @return UUID
    */
  override def openTxnLocal(txnUUID: UUID, partition: Int, onComplete: () => Unit): Unit = {
    stream.metadataStorage.commitEntity.commit(
      streamName = stream.getName,
      partition = partition,
      transaction = txnUUID,
      totalCnt = -1,
      ttl = producerOptions.transactionTTL)

    val msg = ProducerTopicMessage(
      txnUuid = txnUUID,
      ttl = producerOptions.transactionTTL,
      status = ProducerTransactionStatus.opened,
      partition = partition)

    logger.debug(s"Producer ${name} - [GET_LOCAL_TXN PRODUCER] update with msg partition=$partition uuid=${txnUUID.timestamp()} opened")
    subscriberNotifier.publish(msg, onComplete)
  }


  /**
    * Stop this agent
    */
  def stop() = {

    LockUtil.withLockOrDieDo[Unit](threadLock, (100, TimeUnit.SECONDS), Some(logger), () => {
      if (isStop)
        throw new IllegalStateException(s"Producer ${this.name} is already stopped. Duplicate action.")
      isStop = true })

    // stop update state of all open transactions
    shutdownKeepAliveThread.signal(true)
    txnKeepAliveThread.join()
    // stop executor
    backendActivityService.shutdownSafe()
    // stop provide master features to public
    masterP2PAgent.stop()
    // stop function which works with subscribers
    subscriberNotifier.stop()
  }

  /**
    * Agent lock on any actions which has to do with checkpoint
    */
  override def getThreadLock(): ReentrantLock = threadLock

}