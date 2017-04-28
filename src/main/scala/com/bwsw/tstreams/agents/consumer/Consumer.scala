package com.bwsw.tstreams.agents.consumer

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.agents.group.{CheckpointInfo, ConsumerCheckpointInfo, GroupParticipant}
import com.bwsw.tstreams.common.StorageClient
import com.bwsw.tstreams.streams.Stream
import com.bwsw.tstreamstransactionserver.rpc.{ProducerTransaction, TransactionStates}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object Consumer {
  val logger = LoggerFactory.getLogger(this.getClass)
  val SYNC_SLEEP_MS = 100
}

/**
  * Basic consumer class
  *
  * @param name    Name of consumer
  * @param stream  Stream from which to consume transactions
  * @param options Basic consumer options
  */
class Consumer(val name: String,
               val stream: Stream,
               val options: ConsumerOptions)
  extends GroupParticipant with TransactionOperator {

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
  private val transactionBuffer = scala.collection.mutable.Map[Int, scala.collection.mutable.Queue[ConsumerTransaction]]()

  /**
    * Flag which defines either object is running or not
    */
  val isStarted = new AtomicBoolean(false)
  val isStopped = new AtomicBoolean(false)

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

  private def loadNextTransactionsForPartition(partition: Int, currentOffset: Long): mutable.Queue[ConsumerTransaction] = {

    val (last, seq) = stream.client.scanTransactions(stream.name, partition, currentOffset + 1,
      options.transactionGenerator.getTransaction(), options.transactionsPreload, Set())

    val transactionsQueue = mutable.Queue[ConsumerTransaction]()
    seq.foreach(record => {
      val consumerTransaction = new ConsumerTransaction(partition, record.transactionID, record.quantity, record.ttl)
      transactionsQueue.enqueue(consumerTransaction)
    })
    transactionsQueue
  }

  /**
    * Starts the operation.
    */
  def start(): Unit = this.synchronized {
    Consumer.logger.info(s"[INIT] Consumer with name: $name, streamName : ${stream.name}, streamPartitions : ${stream.partitionsCount} is about to start.")

    if (isStopped.get())
      throw new IllegalStateException(s"Consumer $name is stopped already. Unable to start it again.")

    if (isStarted.get())
      throw new IllegalStateException(s"Consumer $name is started already. Double start is detected.")

    /**
      * this is important code. It waits util the server transformed raw commitlog which is now is under construction
      * to rocksdb. This is  important because in that raw log can be checkpoint information that we need to bootstrap
      * the consumer properly.
      *
      * That's why we have to wait while server processes it for sure.
      */
    val offsetsStart = stream.client.getCommitLogOffsets()
    var isSynchronized = false
    while (!isSynchronized) {
      val offsetsNow = stream.client.getCommitLogOffsets()
      if (offsetsNow.currentProcessedCommitLog > offsetsStart.currentConstructedCommitLog)
        isSynchronized = true
      else {
        Thread.sleep(Consumer.SYNC_SLEEP_MS)
      }
    }

    for (partition <- options.readPolicy.getUsedPartitions()) {
      val bootstrapOffset =
        if (stream.client.checkConsumerOffsetExists(name, stream.name, partition) && options.useLastOffset) {
          val off = stream.client.getLastSavedConsumerOffset(name, stream.name, partition)

          if (Consumer.logger.isDebugEnabled())
            Consumer.logger.debug(s"Bootstrap offset load: $off")

          off
        } else {
          val off = options.offset match {
            case Offset.Oldest =>
              options.transactionGenerator.getTransaction(System.currentTimeMillis() - (stream.ttl + 1) * 1000)
            case Offset.Newest =>
              options.transactionGenerator.getTransaction()
            case dateTime: Offset.DateTime =>
              options.transactionGenerator.getTransaction(dateTime.startTime.getTime)
            case offset: Offset.ID =>
              offset.startID
            case _ =>
              throw new IllegalStateException(s"Offset option for consumer $name cannot be resolved to known Offset.* object.")
          }

          if (Consumer.logger.isDebugEnabled())
            Consumer.logger.debug(s"Bootstrap offset historical: $off")

          off
        }
      updateOffsets(partition, bootstrapOffset)
      transactionBuffer(partition) = mutable.Queue[ConsumerTransaction]()
    }

    isStarted.set(true)

    if (options.checkpointAtStart) checkpoint()

  }


  /**
    * Receives new transaction from the partition
    *
    * @param partition
    * @return
    */
  def getTransaction(partition: Int): Option[ConsumerTransaction] = this.synchronized {

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

    // We found invalid transaction, so just skip it and move forward
    if (transaction.getCount() == 0) {
      transactionBuffer(partition).dequeue()
      updateOffsets(partition, transaction.getTransactionID())
      return getTransaction(partition)
    }

    // we found valid transaction transaction
    if (transaction.getCount() != -1) {
      updateOffsets(partition, transaction.getTransactionID())
      transactionBuffer(partition).dequeue()
      transaction.attach(this)
      return Some(transaction)
    }

    // we found open one, try to update it.
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
  def getLastTransaction(partition: Int): Option[ConsumerTransaction] = this.synchronized {
    if (!isStarted.get())
      throw new IllegalStateException(s"Consumer $name is not started. Start it first.")

    val transactionId = stream.client.getLastTransactionId(stream.name, partition)
    if (transactionId > 0) {
      val txn = stream.client.getTransaction(stream.name, partition, transactionId)
      txn.map(t => new ConsumerTransaction(partition = partition, transactionID = t.transactionID, count = t.quantity, ttl = t.ttl))
    } else
      None
  }


  /**
    *
    * @param partition     Partition from which historic transaction will be retrieved
    * @param transactionID ID for this transaction
    * @return BasicConsumerTransaction
    */
  def getTransactionById(partition: Int, transactionID: Long): Option[ConsumerTransaction] = this.synchronized {

    if (!isStarted.get())
      throw new IllegalStateException(s"Consumer $name is not started. Start it first.")

    if (Consumer.logger.isDebugEnabled) {
      Consumer.logger.debug(s"Start retrieving new historic transaction for consumer with" +
        s" name : $name, streamName : ${stream.name}, streamPartitions : ${stream.partitionsCount}")
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
    * @param offset    offset value
    */
  def setStreamPartitionOffset(partition: Int, offset: Long): Unit = this.synchronized {
    if (!isStarted.get())
      throw new IllegalStateException(s"Consumer $name is not started. Start it first.")

    updateOffsets(partition, offset)
  }

  /**
    * Update transaction (if transaction is not closed it will have total packets value -1)
    *
    * @param transactionID Transaction to update
    * @return Updated transaction
    */
  def loadTransactionFromDB(partition: Int, transactionID: Long): Option[ConsumerTransaction] = this.synchronized {
    if (!isStarted.get())
      throw new IllegalStateException("Consumer is not started. Start consumer first.")

    stream.client.getTransaction(stream.name, partition, transactionID)
      .map(rec => new ConsumerTransaction(partition, transactionID, rec.quantity, rec.ttl))
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
        s"consumer with name : $name, streamName : ${stream.name}, streamPartitions : ${stream.partitionsCount}")
    }
    stream.client.saveConsumerOffsetBatch(name, stream.name, checkpointOffsets)
    checkpointOffsets.clear()
  }

  /**
    * Info to commit
    */
  override def getCheckpointInfoAndClear(): List[CheckpointInfo] = this.synchronized {

    if (!isStarted.get())
      throw new IllegalStateException("Consumer is not started. Start consumer first.")

    val checkpointData = checkpointOffsets.map { case (partition, lastTransaction) =>
      ConsumerCheckpointInfo(name, stream.name, partition, lastTransaction)
    }.toList
    checkpointOffsets.clear()
    checkpointData
  }

  /**
    * Agent lock on any actions which has to do with checkpoint
    */
  override def getThreadLock(): ReentrantLock = null

  def stop() = {

    if (isStopped.getAndSet(true))
      throw new IllegalStateException(s"Consumer $name is stopped already. Unable to stop it again.")

    if (!isStarted.getAndSet(false))
      throw new IllegalStateException(s"Consumer $name is not started. Start consumer first.")

    checkpointOffsets.clear()
    currentOffsets.clear()
    transactionBuffer.clear()
    stream.client.shutdown()
  }

  /**
    * Allows to build Transaction without accessing DB
    *
    * @param partition
    * @param transactionID
    * @param count
    * @return
    */
  def buildTransactionObject(partition: Int, transactionID: Long, count: Int): Option[ConsumerTransaction] = {
    val transaction = new ConsumerTransaction(partition, transactionID, count, -1)
    transaction.attach(this)
    Some(transaction)
  }

  override def getTransactionsFromTo(partition: Int, from: Long, to: Long): ListBuffer[ConsumerTransaction] = {
    val set = Set[TransactionStates](TransactionStates.Opened)
    val (_, seq) = stream.client.scanTransactions(stream.name, partition, from + 1, to, options.transactionsPreload, set)
    if(Consumer.logger.isDebugEnabled())
      Consumer.logger.debug(s"scanTransactions(${stream.name}, ${partition}, ${from + 1}, $to, ${options.transactionsPreload}, ${set}) -> $seq")
    val result = ListBuffer[ConsumerTransaction]()
    seq.foreach(rec => {
      if (rec.quantity > 0 && rec.state != TransactionStates.Invalid) {
        val t = new ConsumerTransaction(partition = partition, transactionID = rec.transactionID, count = rec.quantity, ttl = rec.ttl)
        t.attach(this)
        result.append(t)
      }
    })
    result
  }

  override def getProposedTransactionId(): Long = options.transactionGenerator.getTransaction()

  override def getStorageClient(): StorageClient = stream.client
}