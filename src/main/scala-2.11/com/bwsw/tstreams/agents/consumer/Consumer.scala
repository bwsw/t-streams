package com.bwsw.tstreams.agents.consumer

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.agents.group.{GroupParticipant, CheckpointInfo, ConsumerCheckpointInfo}
import com.bwsw.tstreams.metadata.MetadataStorage
import com.bwsw.tstreams.streams.TStream
import org.slf4j.LoggerFactory

object Consumer {
  val logger = LoggerFactory.getLogger(this.getClass)
}

/**
  * Basic consumer class
  *
  * @param name    Name of consumer
  * @param stream  Stream from which to consume transactions
  * @param options Basic consumer options
  * @tparam T User data type
  */
class Consumer[T](val name: String,
                         val stream: TStream[Array[Byte]],
                         val options: Options[T]) extends GroupParticipant {
  /**
    * agent name
    */
  override def getAgentName = name

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
  private val transactionBuffer = scala.collection.mutable.Map[Int, scala.collection.mutable.Queue[Transaction[T]]]()

  /**
    * Indicate set offsets or not
    */
  private var isReadOffsetsAreSet = false

  val isStarted = new AtomicBoolean(false)

  stream.dataStorage.bind()

  def start(): Unit = this.synchronized {
    Consumer.logger.info(s"Start a new consumer with name: ${name}, streamName : ${stream.getName}, streamPartitions : ${stream.getPartitions}")

    if(isStarted.get())
      throw new IllegalStateException(s"Consumer ${name} is started already. Double start is detected.")

    //set consumer offsets
    if (!stream.metadataStorage.consumerEntity.exists(name) || !options.useLastOffset) {
      isReadOffsetsAreSet = true

      for (i <- 0 until stream.getPartitions) {
        currentOffsets(i) = options.offset match {
          case Offset.Oldest =>
            options.txnGenerator.getTimeUUID(0)
          case Offset.Newest =>
            options.txnGenerator.getTimeUUID()
          case dateTime: Offset.DateTime =>
            options.txnGenerator.getTimeUUID(dateTime.startTime.getTime)
          case offset: Offset.UUID =>
            offset.startUUID
          case _ =>
            throw new IllegalStateException(s"Offset option for consumer ${name} cannot be resolved to known Offset.* object.")
        }
        offsetsForCheckpoint(i) =  currentOffsets(i)
      }
    }

    if (!isReadOffsetsAreSet) {
      for (i <- options.readPolicy.getUsedPartitions()) {
        val offset = stream.metadataStorage.consumerEntity.getLastSavedOffset(name, stream.getName, i)
        currentOffsets(i)       = offset
        offsetsForCheckpoint(i) = offset
      }
    }

    for (i <- options.readPolicy.getUsedPartitions())
      transactionBuffer(i) = stream.metadataStorage.commitEntity.getTransactions[T](
        streamName = stream.getName,
        partition = i,
        fromTransaction = currentOffsets(i),
        cnt = options.transactionsPreload)

    isStarted.set(true)
  }

  /**
    * Helper function for getTransaction() method
    *
    * @return BasicConsumerTransaction or None
    */
  private def getNextTransaction(): Option[Transaction[T]] = this.synchronized {

    if(!isStarted.get())
      throw new IllegalStateException(s"Consumer ${name} is not started. Start it first.")

    if (options.readPolicy.isRoundFinished()) {
      return None
    }

    val partition = options.readPolicy.getNextPartition()

    if (transactionBuffer(partition).isEmpty) {
      transactionBuffer(partition) = stream.metadataStorage.commitEntity.getTransactions(
        streamName = stream.getName,
        partition = partition,
        fromTransaction = currentOffsets(partition),
        cnt = options.transactionsPreload)

      if (transactionBuffer(partition).isEmpty) {
        return getNextTransaction()
      }
    }

    val txn = transactionBuffer(partition).head

    if (txn.getCount() != -1) {
      offsetsForCheckpoint(partition) = txn.getTxnUUID()
      currentOffsets(partition)       = txn.getTxnUUID()
      txn.attach(this)
      transactionBuffer(partition).dequeue()
      return Some(txn)
    } else {
      val updatedTxnOpt: Option[Transaction[T]] = updateTransactionInfoFromDB(txn.getTxnUUID(), partition)
      if (updatedTxnOpt.isDefined) {
        val updatedTxn = updatedTxnOpt.get

        if (updatedTxn.getCount() != -1) {
          offsetsForCheckpoint(partition) = txn.getTxnUUID()
          currentOffsets(partition) = txn.getTxnUUID()
          updatedTxn.attach(this)
          transactionBuffer(partition).dequeue()
          return Some(updatedTxn)
        }
      }
    }
    getNextTransaction()
  }

  /**
    * @return Consumed transaction or None if nothing to consume
    */
  def getTransaction(): Option[Transaction[T]] = {

    if(!isStarted.get())
      throw new IllegalStateException(s"Consumer ${name} is not started. Start it first.")

    Consumer.logger.debug(s"Start new transaction for consumer with name : $name, streamName : ${stream.getName}, streamPartitions : ${stream.getPartitions}")
    options.readPolicy.startNewRound()
    val txn: Option[Transaction[T]] = getNextTransaction
    txn
  }

  /**
    * Getting last transaction from concrete partition
    *
    * @param partition Partition to get last transaction
    * @return Last txn
    */
  def getLastTransaction(partition: Int): Option[Transaction[T]] = this.synchronized {

    if(!isStarted.get())
      throw new IllegalStateException(s"Consumer ${name} is not started. Start it first.")

    var curUuid = options.txnGenerator.getTimeUUID()
    var isFinished = false
    while (!isFinished) {

      val queue = stream.metadataStorage.commitEntity.getLastTransactionHelper[T](
        stream.getName,
        partition,
        curUuid)

      if (queue.isEmpty)
        isFinished = true
      else {
        while (queue.nonEmpty) {
          val txn: Transaction[T] = queue.dequeue()
          if (txn.getCount() != -1) {
            return Option[Transaction[T]](txn)
          }
          curUuid = txn.getTxnUUID()
        }
      }
    }
    None
  }

  def getTransactionsFromTo(partition: Int, from: UUID, to: UUID): List[Transaction[T]] = {
    Nil
  }

  /**
    *
    * @param partition Partition from which historic transaction will be retrieved
    * @param uuid      Uuid for this transaction
    * @return BasicConsumerTransaction
    */
  def getTransactionById(partition: Int, uuid: UUID): Option[Transaction[T]] = this.synchronized {

    if(!isStarted.get())
      throw new IllegalStateException(s"Consumer ${name} is not started. Start it first.")

    if(Consumer.logger.isDebugEnabled) {
      Consumer.logger.debug(s"Start retrieving new historic transaction for consumer with" +
        s" name : $name, streamName : ${stream.getName}, streamPartitions : ${stream.getPartitions}")
    }

    val txnOpt = updateTransactionInfoFromDB(uuid, partition)
    if (txnOpt.isDefined) {
      val txn = txnOpt.get
      if (txn.getCount() != -1) {
        txnOpt.get.attach(this)
        txnOpt
      }
      else
        None
    }
    else {
      None
    }
  }

  /**
    * Sets offset on concrete partition
    *
    * @param partition partition to set offset
    * @param uuid      offset value
    */
  def setStreamPartitionOffset(partition: Int, uuid: UUID): Unit = this.synchronized {
    if(!isStarted.get())
      throw new IllegalStateException(s"Consumer ${name} is not started. Start it first.")

    offsetsForCheckpoint(partition) = uuid
    currentOffsets(partition)       = uuid

    transactionBuffer(partition) = stream.metadataStorage.commitEntity.getTransactions(
                                                                            stream.getName,
                                                                            partition,
                                                                            uuid,
                                                                            options.transactionsPreload)
  }

  /**
    * Update transaction (if transaction is not closed it will have total packets value -1)
    *
    * @param txn Transaction to update
    * @return Updated transaction
    */
  def updateTransactionInfoFromDB(txn: UUID, partition: Int): Option[Transaction[T]] = this.synchronized {
    if(!isStarted.get())
      throw new IllegalStateException("Consumer is not started. Start consumer first.")

    val data: Option[(Int, Int)] =
      stream.metadataStorage.commitEntity.getTransactionItemCountAndTTL(
        stream.getName,
        partition,
        txn)

    if (data.isDefined) {
      val (cnt, ttl) = data.get
      Some(new Transaction(partition, txn, cnt, ttl))
    }
    else
      None
  }

  /**
    * Save current offsets in metadata
    * to read later from them (in case of system stop/failure)
    */
  def checkpoint(): Unit = this.synchronized {
    if(!isStarted.get())
      throw new IllegalStateException("Consumer is not started. Start consumer first.")

    if (Consumer.logger.isDebugEnabled) {
      Consumer.logger.debug(s"Start saving checkpoints for " +
        s"consumer with name : $name, streamName : ${stream.getName}, streamPartitions : ${stream.getPartitions}")
    }
    stream.metadataStorage.consumerEntity.saveBatchOffset(name, stream.getName, offsetsForCheckpoint)
    offsetsForCheckpoint.clear()
  }

  /**
    * Info to commit
    */
  override def getCheckpointInfoAndClear(): List[CheckpointInfo] = this.synchronized {

    if(!isStarted.get())
      throw new IllegalStateException("Consumer is not started. Start consumer first.")

    val checkpointData = offsetsForCheckpoint.map { case (partition, lastTxn) =>
      ConsumerCheckpointInfo(name, stream.getName, partition, lastTxn)
    }.toList
    offsetsForCheckpoint.clear()
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
  override def getThreadLock(): ReentrantLock = null

  def stop() = {
    if (!isStarted.getAndSet(false))
      throw new IllegalStateException("Consumer is not started. Start consumer first.")
  }
}