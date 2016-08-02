package com.bwsw.tstreams.agents.consumer

import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.agents.group.{Agent, CheckpointInfo, ConsumerCheckpointInfo}
import com.bwsw.tstreams.common.LockUtil
import com.bwsw.tstreams.entities.TransactionSettings
import com.bwsw.tstreams.metadata.MetadataStorage
import com.bwsw.tstreams.streams.BasicStream
import org.slf4j.LoggerFactory


/**
  * Basic consumer class
  *
  * @param name    Name of consumer
  * @param stream  Stream from which to consume transactions
  * @param options Basic consumer options
  * @tparam USERTYPE User data type
  */
class BasicConsumer[USERTYPE](val name: String,
                                        val stream: BasicStream[Array[Byte]],
                                        val options: BasicConsumerOptions[USERTYPE]) extends Agent {

  stream.dataStorage.bind()

  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * agent name
    */
  override def getAgentName = name

  /**
    * Lock for managing transactions
    */
  private val consumerLock = new ReentrantLock(true)

  /**
    * Temporary checkpoints (will be cleared after every checkpoint() invokes)
    */
  private val offsetsForCheckpoint = scala.collection.mutable.Map[Int, UUID]()

  /**
    * Local offsets
    */
  protected val currentOffsets = scala.collection.mutable.Map[Int, UUID]()

  /**
    * Buffer for transactions preload
    */
  private val transactionBuffer = scala.collection.mutable.Map[Int, scala.collection.mutable.Queue[TransactionSettings]]()

  /**
    * Indicate set offsets or not
    */
  private var isReadOffsetsAreSet = false

  val isStarted = new AtomicBoolean(false)


  def start(): Unit = {
    logger.info(s"Start a new consumer with name :${name}, streamName : ${stream.getName}, streamPartitions : ${stream.getPartitions}")
    LockUtil.lockOrDie(consumerLock, (100, TimeUnit.SECONDS), Some(logger))
    //set consumer offsets
    if (!stream.metadataStorage.consumerEntity.exist(name) || !options.useLastOffset) {
      isReadOffsetsAreSet = true

      options.offset match {
        case Offsets.Oldest =>
          for (i <- 0 until stream.getPartitions) {
            currentOffsets(i) = options.txnGenerator.getTimeUUID(0)
            offsetsForCheckpoint(i) = options.txnGenerator.getTimeUUID(0)
          }

        case Offsets.Newest =>
          val newestUuid = options.txnGenerator.getTimeUUID()
          for (i <- 0 until stream.getPartitions) {
            currentOffsets(i) = newestUuid
            offsetsForCheckpoint(i) = newestUuid
          }

        case dateTime: Offsets.DateTime =>
          for (i <- 0 until stream.getPartitions) {
            currentOffsets(i) = options.txnGenerator.getTimeUUID(dateTime.startTime.getTime)
            offsetsForCheckpoint(i) = options.txnGenerator.getTimeUUID(dateTime.startTime.getTime)
          }

        case offset: Offsets.CustomUUID =>
          for (i <- 0 until stream.getPartitions) {
            currentOffsets(i) = offset.startUUID
            offsetsForCheckpoint(i) = offset.startUUID
          }

        case _ => {
          consumerLock.unlock()
          throw new IllegalStateException("offset cannot be resolved")
        }
      }

    }

    if (!isReadOffsetsAreSet) {
      for (i <- options.readPolicy.getUsedPartitions()) {
        val offset = stream.metadataStorage.consumerEntity.getOffset(name, stream.getName, i)
        offsetsForCheckpoint(i) = offset
        currentOffsets(i) = offset
      }
    }

    for (i <- options.readPolicy.getUsedPartitions())
      transactionBuffer(i) = stream.metadataStorage.commitEntity.getTransactions(
        streamName = stream.getName,
        partition = i,
        lastTransaction = currentOffsets(i),
        cnt = options.transactionsPreload)

    isStarted.set(true)
    consumerLock.unlock()
  }

  /**
    * Helper function for getTransaction() method
    *
    * @return BasicConsumerTransaction or None
    */
  private def getTxnOpt: Option[BasicConsumerTransaction[USERTYPE]] = {

    if(!isStarted.get())
      throw new IllegalStateException("Start consumer first.")

    LockUtil.lockOrDie(consumerLock, (100, TimeUnit.SECONDS), Some(logger))

    if (options.readPolicy.isRoundFinished()) {
      consumerLock.unlock()
      return None
    }

    val curPartition = options.readPolicy.getNextPartition

    if (transactionBuffer(curPartition).isEmpty) {
      transactionBuffer(curPartition) = stream.metadataStorage.commitEntity.getTransactions(
        streamName = stream.getName,
        partition = curPartition,
        lastTransaction = currentOffsets(curPartition),
        cnt = options.transactionsPreload)
    }

    if (transactionBuffer(curPartition).isEmpty) {
      consumerLock.unlock()
      return getTxnOpt
    }

    val txn: TransactionSettings = transactionBuffer(curPartition).front

    if (txn.totalItems != -1) {
      offsetsForCheckpoint(curPartition) = txn.txnUuid
      currentOffsets(curPartition) = txn.txnUuid
      transactionBuffer(curPartition).dequeue()
      consumerLock.unlock()
      return Some(new BasicConsumerTransaction[USERTYPE](this, curPartition, txn))
    }

    val updatedTxnOpt: Option[TransactionSettings] = updateTransaction(txn.txnUuid, curPartition)

    if (updatedTxnOpt.isDefined) {
      val updatedTxn = updatedTxnOpt.get

      if (updatedTxn.totalItems != -1) {
        offsetsForCheckpoint(curPartition) = txn.txnUuid
        currentOffsets(curPartition) = txn.txnUuid
        transactionBuffer(curPartition).dequeue()
        consumerLock.unlock()
        return Some(new BasicConsumerTransaction[USERTYPE](this, curPartition, updatedTxn))
      }
    }
    else
      transactionBuffer(curPartition).dequeue()

    consumerLock.unlock()
    getTxnOpt
  }

  /**
    * @return Consumed transaction or None if nothing to consume
    */
  def getTransaction: Option[BasicConsumerTransaction[USERTYPE]] = {
    if(!isStarted.get())
      throw new IllegalStateException("Start consumer first.")

    LockUtil.lockOrDie(consumerLock, (100, TimeUnit.SECONDS), Some(logger))
    logger.debug(s"Start new transaction for consumer with name : $name, streamName : ${stream.getName}, streamPartitions : ${stream.getPartitions}")

    options.readPolicy.startNewRound()
    val txn: Option[BasicConsumerTransaction[USERTYPE]] = getTxnOpt
    consumerLock.unlock()
    txn
  }

  /**
    * Getting last transaction from concrete partition
    *
    * @param partition Partition to get last transaction
    * @return Last txn
    */
  def getLastTransaction(partition: Int): Option[BasicConsumerTransaction[USERTYPE]] = {
    if(!isStarted.get())
      throw new IllegalStateException("Start consumer first.")

    LockUtil.lockOrDie(consumerLock, (100, TimeUnit.SECONDS), Some(logger))
    var curUuid = options.txnGenerator.getTimeUUID()
    var isFinished = false
    while (!isFinished) {
      val queue = stream.metadataStorage.commitEntity.getLastTransactionHelper(
        stream.getName,
        partition,
        curUuid)
      if (queue.isEmpty)
        isFinished = true
      else {
        while (queue.nonEmpty) {
          val txn = queue.dequeue()
          if (txn.totalItems != -1) {
            consumerLock.unlock()
            return Some(new BasicConsumerTransaction[USERTYPE](this, partition, txn))
          }
          curUuid = txn.txnUuid
        }
      }
    }
    consumerLock.unlock()
    None
  }

  /**
    *
    * @param partition Partition from which historic transaction will be retrieved
    * @param uuid      Uuid for this transaction
    * @return BasicConsumerTransaction
    */
  def getTransactionById(partition: Int, uuid: UUID): Option[BasicConsumerTransaction[USERTYPE]] = {
    if(!isStarted.get())
      throw new IllegalStateException("Start consumer first.")

    LockUtil.lockOrDie(consumerLock, (100, TimeUnit.SECONDS), Some(logger))

    logger.debug(s"Start retrieving new historic transaction for consumer with" +
      s" name : $name, streamName : ${stream.getName}, streamPartitions : ${stream.getPartitions}")
    val txnOpt = updateTransaction(uuid, partition)
    val res =
      if (txnOpt.isDefined) {
        val txn = txnOpt.get
        if (txn.totalItems != -1)
          Some(new BasicConsumerTransaction[USERTYPE](this, partition, txn))
        else
          None
      }
      else {
        None
      }
    consumerLock.unlock()
    res
  }

  /**
    * Sets offset on concrete partition
    *
    * @param partition partition to set offset
    * @param uuid      offset value
    */
  def setLocalOffset(partition: Int, uuid: UUID): Unit = {
    if(!isStarted.get())
      throw new IllegalStateException("Start consumer first.")

    LockUtil.lockOrDie(consumerLock, (100, TimeUnit.SECONDS), Some(logger))
    offsetsForCheckpoint(partition) = uuid
    currentOffsets(partition) = uuid
    transactionBuffer(partition) = stream.metadataStorage.commitEntity.getTransactions(
      stream.getName,
      partition,
      uuid,
      options.transactionsPreload)
    consumerLock.unlock()
  }

  /**
    * Update transaction (if transaction is not closed it will have total packets value -1)
    *
    * @param txn Transaction to update
    * @return Updated transaction
    */
  def updateTransaction(txn: UUID, partition: Int): Option[TransactionSettings] = {
    if(!isStarted.get())
      throw new IllegalStateException("Start consumer first.")

    LockUtil.lockOrDie(consumerLock, (100, TimeUnit.SECONDS), Some(logger))
    val amount: Option[(Int, Int)] = stream.metadataStorage.commitEntity.getTransactionAmount(
      stream.getName,
      partition,
      txn)
    consumerLock.unlock()
    if (amount.isDefined) {
      val (cnt, ttl) = amount.get
      Some(TransactionSettings(txn, cnt, ttl))
    }
    else
      None
  }

  /**
    * Save current offsets in metadata
    * to read later from them (in case of system stop/failure)
    */
  def checkpoint(): Unit = {
    if(!isStarted.get())
      throw new IllegalStateException("Start consumer first.")

    LockUtil.lockOrDie(consumerLock, (100, TimeUnit.SECONDS), Some(logger))
    logger.info(s"Start saving checkpoints for " +
      s"consumer with name : $name, streamName : ${stream.getName}, streamPartitions : ${stream.getPartitions}")

    stream.metadataStorage.consumerEntity.saveBatchOffset(name, stream.getName, offsetsForCheckpoint)
    offsetsForCheckpoint.clear()
    consumerLock.unlock()
  }

  /**
    * Info to commit
    */
  override def getCheckpointInfoAndClear(): List[CheckpointInfo] = {
    if(!isStarted.get())
      throw new IllegalStateException("Start consumer first.")

    LockUtil.lockOrDie(consumerLock, (100, TimeUnit.SECONDS), Some(logger))
    val checkpointData = offsetsForCheckpoint.map { case (partition, lastTxn) =>
      ConsumerCheckpointInfo(name, stream.getName, partition, lastTxn)
    }.toList
    offsetsForCheckpoint.clear()
    consumerLock.unlock()
    checkpointData
  }

  /**
    * @return Metadata storage link for concrete agent
    */
  override def getMetadataRef(): MetadataStorage =
    stream.metadataStorage

  /**
    * Agent lock on any actions which has to do with checkpoint
    */
  override def getThreadLock(): ReentrantLock = consumerLock

  def stop() = {
    if (!isStarted.get())
      throw new IllegalStateException("Consumer is not started")
    isStarted.set(false)
  }
}