package com.bwsw.tstreams.agents.consumer

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.agents.group.{CheckpointInfo, ConsumerCheckpointInfo, GroupParticipant}
import com.bwsw.tstreams.metadata.{MetadataStorage, TransactionDatabase, TransactionRecord}
import com.bwsw.tstreams.streams.Stream
import org.slf4j.LoggerFactory

import scala.collection.mutable
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
                  val stream: Stream[Array[Byte]],
                  val options: ConsumerOptions[T])
  extends GroupParticipant
    with TransactionOperator[T] {

  val tsdb = new TransactionDatabase(stream.getMetadataStorage().getSession(), stream.getName())

  /**
    * Temporary checkpoints (will be cleared after every checkpoint() invokes)
    */
  private val checkpointOffsets = scala.collection.mutable.Map[Int, Long]()

  /**
    * Local offsets
    */
  protected val currentOffsets = scala.collection.mutable.Map[Int, Long]()

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

  stream.dataStorage.bind()

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
  def getCurrentOffset(partition: Int): Long = this.synchronized {
    currentOffsets(partition)
  }

  private def loadNextTransactionsForPartition(partition: Int, currentOffset: Long): mutable.Queue[ConsumerTransaction[T]] = {
    var count: Int = 0
    val transactionsRecords = tsdb.scanForward(partition, currentOffset, options.transactionGenerator.getTransaction()) (r => {
      count += 1
      count <= options.transactionsPreload
    })

    val transactionsQueue = mutable.Queue[ConsumerTransaction[T]]()
    transactionsRecords.foreach(record => {
      val consumerTransaction = new ConsumerTransaction[T](partition, record.transactionID, record.count, record.ttl)
      transactionsQueue.enqueue(consumerTransaction)
    })
    transactionsQueue
  }

  /**
    * Starts the operation.
    */
  def start(): Unit = this.synchronized {
    Consumer.logger.info(s"[INIT] Consumer with name: $name, streamName : ${stream.getName}, streamPartitions : ${stream.getPartitions} is about to start.")

    if (isStarted.get())
      throw new IllegalStateException(s"Consumer $name is started already. Double start is detected.")

    //set consumer offsets
    if (!stream.metadataStorage.consumerEntity.exists(name) || !options.useLastOffset) {
      isReadOffsetsAreSet = true

      for (i <- 0 until stream.getPartitions) {
        currentOffsets(i) = options.offset match {
          case Offset.Oldest =>
            options.transactionGenerator.getTransaction(0)
          case Offset.Newest =>
            options.transactionGenerator.getTransaction()
          case dateTime: Offset.DateTime =>
            options.transactionGenerator.getTransaction(dateTime.startTime.getTime)
          case offset: Offset.ID =>
            offset.startID
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

    for (partition <- options.readPolicy.getUsedPartitions())
      transactionBuffer(partition) = loadNextTransactionsForPartition(partition, currentOffsets(partition))

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
      transactionBuffer(partition) = loadNextTransactionsForPartition(partition, currentOffsets(partition))

      if (transactionBuffer(partition).isEmpty) {
        return None
      }
    }

    val transaction = transactionBuffer(partition).head

    if (transaction.getCount() != -1) {
      updateOffsets(partition, transaction.getTransactionID())
      transactionBuffer(partition).dequeue()
      transaction.attach(this)
      return Some(transaction)
    }

    val transactionUpdatedOpt = getTransactionById(partition, transaction.getTransactionID())
    if (transactionUpdatedOpt.isDefined) {
      updateOffsets(partition, transactionUpdatedOpt.get.getTransactionID())
      transactionBuffer(partition).dequeue()
      return transactionUpdatedOpt
    }

    return None
  }

  private def updateOffsets(partition: Int, id: Long) = {
    checkpointOffsets(partition) = id
    currentOffsets(partition) = id
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

    val transactionFrom = new java.lang.Long(options.transactionGenerator.getTransaction())
    val transactionsRecord = tsdb.searchBackward(new Integer(partition), transactionFrom, new java.lang.Long(0)) (rec => rec.count > 0)

    transactionsRecord
      .map(rec => new ConsumerTransaction[T](partition = partition, transactionID = rec.transactionID, count = rec.count, ttl = rec.ttl))
  }



  /**
    *
    * @param partition Partition from which historic transaction will be retrieved
    * @param transactionID      ID for this transaction
    * @return BasicConsumerTransaction
    */
  def getTransactionById(partition: Int, transactionID: Long): Option[ConsumerTransaction[T]] = this.synchronized {

    if (!isStarted.get())
      throw new IllegalStateException(s"Consumer $name is not started. Start it first.")

    if (Consumer.logger.isDebugEnabled) {
      Consumer.logger.debug(s"Start retrieving new historic transaction for consumer with" +
        s" name : $name, streamName : ${stream.getName}, streamPartitions : ${stream.getPartitions}")
    }

    val transactionOpt = loadTransactionFromDB(partition, transactionID)
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
    * @param offset      offset value
    */
  def setStreamPartitionOffset(partition: Int, offset: Long): Unit = this.synchronized {
    if (!isStarted.get())
      throw new IllegalStateException(s"Consumer $name is not started. Start it first.")

    updateOffsets(partition, offset)

    transactionBuffer(partition) = loadNextTransactionsForPartition(partition, offset)
  }

  /**
    * Update transaction (if transaction is not closed it will have total packets value -1)
    *
    * @param transactionID Transaction to update
    * @return Updated transaction
    */
  def loadTransactionFromDB(partition: Int, transactionID: Long): Option[ConsumerTransaction[T]] = this.synchronized {
    if (!isStarted.get())
      throw new IllegalStateException("Consumer is not started. Start consumer first.")

    tsdb.get(partition, transactionID)
      .map(rec => new ConsumerTransaction(partition, transactionID, rec.count, rec.ttl))
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
    * @param transactionID
    * @param count
    * @return
    */
  def buildTransactionObject(partition: Int, transactionID: Long, count: Int): Option[ConsumerTransaction[T]] = {
    val transaction = new ConsumerTransaction[T](partition, transactionID, count, -1)
    transaction.attach(this)
    Some(transaction)
  }

  override def getTransactionsFromTo(partition: Int, from: Long, to: Long): ListBuffer[ConsumerTransaction[T]] = {
    val data = tsdb.scanForward(partition, from, to)(transaction => transaction.count > 0)
    val result = ListBuffer[ConsumerTransaction[T]]()
    data.foreach(rec =>
      result.append(new ConsumerTransaction[T](partition = partition, transactionID = rec.transactionID, count = rec.count, ttl = rec.ttl)))
    result
  }
}