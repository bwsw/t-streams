package com.bwsw.tstreams.agents.consumer

import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreams.agents.group.{ConsumerStateInfo, GroupParticipant, StateInfo}
import com.bwsw.tstreams.generator.TransactionGenerator
import com.bwsw.tstreams.storage.StorageClient
import com.bwsw.tstreams.streams.Stream
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
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
  extends GroupParticipant with TransactionOperator with AutoCloseable {

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

  private[tstreams] val transactionGenerator = new TransactionGenerator(stream.client)

  /**
    * Flag which defines either object is running or not
    */
  private val isStarted = new AtomicBoolean(false)
  private val isStopped = new AtomicBoolean(false)

  /**
    * agent name
    */
  override def getAgentName() = name

  /**
    * returns partitions
    */
  def getPartitions: Set[Int] = options.readPolicy.getUsedPartitions


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

    val (last, seq) = stream.client.scanTransactions(stream.id, partition, currentOffset + 1,
      transactionGenerator.getTransaction(), options.transactionsPreload, Set())

    val transactionsQueue = mutable.Queue[ConsumerTransaction]()
    seq.foreach(record => {
      val consumerTransaction = new ConsumerTransaction(partition, record.transactionID, record.quantity, record.state, record.ttl)
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

    for (partition <- options.readPolicy.getUsedPartitions) {
      val bootstrapOffset =
        if (stream.client.checkConsumerOffsetExists(name, stream.id, partition) && options.useLastOffset) {
          val off = stream.client.getLastSavedConsumerOffset(name, stream.id, partition)

          if (Consumer.logger.isDebugEnabled())
            Consumer.logger.debug(s"Bootstrap offset load: $off")

          off
        } else {
          val off = options.offset match {
            case Offset.Oldest =>
              transactionGenerator.getTransaction(System.currentTimeMillis() - (stream.ttl + 1) * 1000)
            case Offset.Newest =>
              transactionGenerator.getTransaction()
            case dateTime: Offset.DateTime =>
              transactionGenerator.getTransaction(dateTime.startTime.getTime)
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

    if (!getPartitions.contains(partition))
      throw new IllegalStateException(s"Consumer doesn't work on partition=$partition.")

    if (transactionBuffer(partition).isEmpty) {
      transactionBuffer(partition) = loadNextTransactionsForPartition(partition, currentOffsets(partition))

      if (transactionBuffer(partition).isEmpty) {
        return None
      }
    }

    val transaction = transactionBuffer(partition).head

    // We found invalid transaction, so just skip it and move forward
    if (transaction.getState == TransactionStates.Invalid) {
      transactionBuffer(partition).dequeue()
      updateOffsets(partition, transaction.getTransactionID)
      return getTransaction(partition)
    }

    // we found valid transaction transaction
    if (transaction.getState != TransactionStates.Opened) {
      updateOffsets(partition, transaction.getTransactionID)
      transactionBuffer(partition).dequeue()
      transaction.attach(this)
      return Some(transaction)
    }

    // we found open one, try to update it.
    val transactionUpdatedOpt = getTransactionById(partition, transaction.getTransactionID)
    if (transactionUpdatedOpt.isDefined && transactionUpdatedOpt.get.getState == TransactionStates.Checkpointed) {
      updateOffsets(partition, transactionUpdatedOpt.get.getTransactionID)
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

    val transactionId = stream.client.getLastTransactionId(stream.id, partition)
    if (transactionId > 0) {
      val txn = stream.client.getTransaction(stream.id, partition, transactionId)
      txn.map(t => new ConsumerTransaction(partition = partition, transactionID = t.transactionID, count = t.quantity, state = t.state, ttl = t.ttl))
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
      if (transaction.getCount != -1) {
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

    stream.client.getTransaction(stream.id, partition, transactionID)
      .map(rec => new ConsumerTransaction(partition, transactionID, rec.quantity, rec.state, rec.ttl))
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
    stream.client.saveConsumerOffsetBatch(name, stream.id, checkpointOffsets)
    checkpointOffsets.clear()
  }

  /**
    * Info to commit
    */
  override def getCheckpointInfoAndClear(): List[StateInfo] = this.synchronized {

    if (!isStarted.get())
      throw new IllegalStateException("Consumer is not started. Start consumer first.")

    val checkpointData = checkpointOffsets.map { case (partition, lastTransaction) =>
      ConsumerStateInfo(name, stream.id, partition, lastTransaction)
    }.toList
    checkpointOffsets.clear()
    checkpointData
  }

  def stop() = {
    try {
      if (isStopped.getAndSet(true))
        throw new IllegalStateException(s"Consumer $name is stopped already. Unable to stop it again.")

      if (!isStarted.getAndSet(false))
        throw new IllegalStateException(s"Consumer $name is not started. Start consumer first.")

      checkpointOffsets.clear()
      currentOffsets.clear()
      transactionBuffer.clear()
    } finally {
      stream.shutdown()
    }
  }

  /**
    * Allows to build Transaction without accessing DB
    *
    * @param partition
    * @param transactionID
    * @param count
    * @return
    */
  def buildTransactionObject(partition: Int, transactionID: Long, state: TransactionStates, count: Int): Option[ConsumerTransaction] = {
    val transaction = new ConsumerTransaction(partition, transactionID, count, state, -1)
    transaction.attach(this)
    Some(transaction)
  }

  override def getTransactionsFromTo(partition: Int, from: Long, to: Long): ListBuffer[ConsumerTransaction] = {
    val set = Set[TransactionStates](TransactionStates.Opened)
    val (_, seq) = stream.client.scanTransactions(stream.id, partition, from + 1, to, options.transactionsPreload, set)
    if(Consumer.logger.isDebugEnabled())
      Consumer.logger.debug(s"scanTransactions(${stream.name}, $partition, ${from + 1}, $to, ${options.transactionsPreload}, $set) -> $seq")
    val result = ListBuffer[ConsumerTransaction]()
    seq.foreach(rec => {
      val t = new ConsumerTransaction(partition = partition, transactionID = rec.transactionID, count = rec.quantity, state = rec.state, ttl = rec.ttl)
      t.attach(this)
      result.append(t)
    })
    result
  }

  override def getProposedTransactionId: Long = transactionGenerator.getTransaction()

  override def getStorageClient(): StorageClient = stream.client

  override def close(): Unit = if(!isStopped.get() && isStarted.get()) stop()
}