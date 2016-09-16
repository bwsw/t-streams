package com.bwsw.tstreams.agents.consumer

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.agents.group.{CheckpointInfo, ConsumerCheckpointInfo, GroupParticipant}
import com.bwsw.tstreams.common.UUIDComparator
import com.bwsw.tstreams.metadata.MetadataStorage
import com.bwsw.tstreams.streams.TStream
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

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
                  val options: ConsumerOptions[T])
  extends GroupParticipant
    with TransactionOperator[T] {


  /**
    * Temporary checkpoints (will be cleared after every checkpoint() invokes)
    */
  private val checkpointOffsets = scala.collection.mutable.Map[Int, UUID]()

  /**
    * Local offsets
    */
  protected val currentOffsets = scala.collection.mutable.Map[Int, UUID]()

  /**
    * Buffer for transactions preload
    */
  private val transactionBuffer = scala.collection.mutable.Map[Int, scala.collection.mutable.Queue[ConsumerTransaction[T]]]()

  /**
    * Indicate set offsets or not
    */
  private var isReadOffsetsAreSet = false

  /**
    * Flag which defines either object is running or not
    */
  val isStarted = new AtomicBoolean(false)

  /**
    * agent name
    */
  override def getAgentName() = name

  /**
    * returns partitions
    */
  def getPartitions(): Set[Int] = options.readPolicy.getUsedPartitions().toSet

  /**
    * returns current read offset
    *
    * @param partition
    * @return
    */
  def getCurrentOffset(partition: Int): UUID = currentOffsets(partition)

  stream.dataStorage.bind()

  /**
    * Starts the operation.
    */
  def start(): Unit = this.synchronized {
    Consumer.logger.info(s"Start a new consumer with name: $name, streamName : ${stream.getName}, streamPartitions : ${stream.getPartitions}")




    if (isStarted.get())
      throw new IllegalStateException(s"Consumer $name is started already. Double start is detected.")

    //set consumer offsets
    if (!stream.metadataStorage.consumerEntity.exists(name) || !options.useLastOffset) {
      isReadOffsetsAreSet = true

      for (i <- 0 until stream.getPartitions) {
        currentOffsets(i) = options.offset match {
          case Offset.Oldest =>
            options.transactionGenerator.getTimeUUID(0)
          case Offset.Newest =>
            options.transactionGenerator.getTimeUUID()
          case dateTime: Offset.DateTime =>
            options.transactionGenerator.getTimeUUID(dateTime.startTime.getTime)
          case offset: Offset.UUID =>
            offset.startUUID
          case _ =>
            throw new IllegalStateException(s"Offset option for consumer $name cannot be resolved to known Offset.* object.")
        }
        checkpointOffsets(i) = currentOffsets(i)
      }
    }

    if (!isReadOffsetsAreSet) {
      for (i <- options.readPolicy.getUsedPartitions()) {
        val offset = stream.metadataStorage.consumerEntity.getLastSavedOffset(name, stream.getName, i)
        updateOffsets(i, offset)
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
    * Receives new transaction from the partition
    *
    * @param partition
    * @return
    */
  def getTransaction(partition: Int): Option[ConsumerTransaction[T]] = this.synchronized {

    if (!isStarted.get())
      throw new IllegalStateException(s"Consumer $name is not started. Start it first.")

    if (!getPartitions().contains(partition))
      throw new IllegalStateException(s"Consumer doesn't work on partition=$partition.")

    if (transactionBuffer(partition).isEmpty) {
      transactionBuffer(partition) = stream.metadataStorage.commitEntity.getTransactions(
        streamName = stream.getName,
        partition = partition,
        fromTransaction = currentOffsets(partition),
        cnt = options.transactionsPreload)

      if (transactionBuffer(partition).isEmpty) {
        return None
      }
    }

    val transaction = transactionBuffer(partition).head

    if (transaction.getCount() != -1) {
      updateOffsets(partition, transaction.getTransactionUUID())
      transactionBuffer(partition).dequeue()
      transaction.attach(this)
      return Some(transaction)
    }

    val transactionUpdatedOpt = getTransactionById(partition, transaction.getTransactionUUID())
    if (transactionUpdatedOpt.isDefined) {
      updateOffsets(partition, transactionUpdatedOpt.get.getTransactionUUID())
      transactionBuffer(partition).dequeue()
      return transactionUpdatedOpt
    }

    return None
  }

  private def updateOffsets(partition: Int, uuid: UUID) = {
    checkpointOffsets(partition) = uuid
    currentOffsets(partition) = uuid
  }

  /**
    * Getting last transaction from concrete partition
    *
    * @param partition Partition to get last transaction
    * @return Last transaction
    */
  def getLastTransaction(partition: Int): Option[ConsumerTransaction[T]] = this.synchronized {

    if (!isStarted.get())
      throw new IllegalStateException(s"Consumer $name is not started. Start it first.")

    var uuid = options.transactionGenerator.getTimeUUID()
    var isFinished = false
    while (!isFinished) {

      val queue = stream.metadataStorage.commitEntity.getLastTransactionHelper[T](stream.getName, partition, uuid)

      if (queue.isEmpty)
        isFinished = true
      else {
        while (queue.nonEmpty) {
          val transaction: ConsumerTransaction[T] = queue.dequeue()
          if (transaction.getCount() != -1) {
            transaction.attach(this)
            return Option[ConsumerTransaction[T]](transaction)
          }
          uuid = transaction.getTransactionUUID()
        }
      }
    }
    None
  }

  def getTransactionsFromTo(partition: Int, from: UUID, to: UUID): ListBuffer[ConsumerTransaction[T]] = {
    val transactions = stream.metadataStorage.commitEntity.getTransactions[T](
      streamName = stream.getName,
      partition = partition,
      fromTransaction = from,
      cnt = options.transactionsPreload)
    val okList = ListBuffer[ConsumerTransaction[T]]()
    var addFlag = true
    var moreItems = true
    while (addFlag && moreItems) {
      if (transactions.isEmpty) {
        moreItems = false
      } else {
        val t = transactions.dequeue()
        if (UUIDComparator.compare(to, t.getTransactionUUID()) > -1) {
          if (t.getCount() >= 0)
            okList.append(t)
          else
            addFlag = false // we have reached uncompleted transaction, stop
        } else {
          // we have reached right end of interval [from, -> to]
          moreItems = false
          addFlag = false
        }
      }
    }
    if (okList.nonEmpty && addFlag && UUIDComparator.compare(to, okList.last.getTransactionUUID()) == 1)
      okList.appendAll(if (okList.nonEmpty) getTransactionsFromTo(partition, okList.last.getTransactionUUID(), to) else ListBuffer[ConsumerTransaction[T]]())

    okList
  }

  /**
    *
    * @param partition Partition from which historic transaction will be retrieved
    * @param uuid      Uuid for this transaction
    * @return BasicConsumerTransaction
    */
  def getTransactionById(partition: Int, uuid: UUID): Option[ConsumerTransaction[T]] = this.synchronized {

    if (!isStarted.get())
      throw new IllegalStateException(s"Consumer $name is not started. Start it first.")

    if (Consumer.logger.isDebugEnabled) {
      Consumer.logger.debug(s"Start retrieving new historic transaction for consumer with" +
        s" name : $name, streamName : ${stream.getName}, streamPartitions : ${stream.getPartitions}")
    }

    val transactionOpt = loadTransactionFromDB(partition, uuid)
    if (transactionOpt.isDefined) {
      val transaction = transactionOpt.get
      if (transaction.getCount() != -1) {
        transactionOpt.get.attach(this)
        transactionOpt
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
    if (!isStarted.get())
      throw new IllegalStateException(s"Consumer $name is not started. Start it first.")

    updateOffsets(partition, uuid)

    transactionBuffer(partition) = stream.metadataStorage.commitEntity.getTransactions(
      stream.getName,
      partition,
      uuid,
      options.transactionsPreload)
  }

  /**
    * Update transaction (if transaction is not closed it will have total packets value -1)
    *
    * @param transactionUUID Transaction to update
    * @return Updated transaction
    */
  def loadTransactionFromDB(partition: Int, transactionUUID: UUID): Option[ConsumerTransaction[T]] = this.synchronized {
    if (!isStarted.get())
      throw new IllegalStateException("Consumer is not started. Start consumer first.")

    val data: Option[(Int, Int)] =
      stream.metadataStorage.commitEntity.getTransactionItemCountAndTTL(stream.getName, partition, transactionUUID)

    if (data.isDefined) {
      val (cnt, ttl) = data.get
      Some(new ConsumerTransaction(partition, transactionUUID, cnt, ttl))
    }
    else
      None
  }

  /**
    * Save current offsets in metadata
    * to read later from them (in case of system stop/failure)
    */
  def checkpoint(): Unit = this.synchronized {
    if (!isStarted.get())
      throw new IllegalStateException("Consumer is not started. Start consumer first.")

    if (Consumer.logger.isDebugEnabled) {
      Consumer.logger.debug(s"Start saving checkpoints for " +
        s"consumer with name : $name, streamName : ${stream.getName}, streamPartitions : ${stream.getPartitions}")
    }
    stream.metadataStorage.consumerEntity.saveBatchOffset(name, stream.getName, checkpointOffsets)
    checkpointOffsets.clear()
  }

  /**
    * Info to commit
    */
  override def getCheckpointInfoAndClear(): List[CheckpointInfo] = this.synchronized {

    if (!isStarted.get())
      throw new IllegalStateException("Consumer is not started. Start consumer first.")

    val checkpointData = checkpointOffsets.map { case (partition, lastTransaction) =>
      ConsumerCheckpointInfo(name, stream.getName, partition, lastTransaction)
    }.toList
    checkpointOffsets.clear()
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

    checkpointOffsets.clear()
    currentOffsets.clear()
    transactionBuffer.clear()
  }

  /**
    * Allows to build Transaction without accessing DB
    *
    * @param partition
    * @param uuid
    * @param count
    * @return
    */
  def buildTransactionObject(partition: Int, uuid: UUID, count: Int): Option[ConsumerTransaction[T]] = {
    val transaction = new ConsumerTransaction[T](partition, uuid, count, -1)
    transaction.attach(this)
    Some(transaction)
  }
}