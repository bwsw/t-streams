package com.bwsw.tstreams.agents.consumer

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.agents.consumer.subscriber.Subscriber
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
                         val options: Options[T])
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
  private val transactionBuffer = scala.collection.mutable.Map[Int, scala.collection.mutable.Queue[Transaction[T]]]()

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
    * @param partition
    * @return
    */
  def getCurrentOffset(partition: Int): UUID = currentOffsets(partition)

  stream.dataStorage.bind()

  /**
    * Starts the operation.
    */
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
        checkpointOffsets(i) =  currentOffsets(i)
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
    * @param partition
    * @return
    */
  def getTransaction(partition: Int): Option[Transaction[T]] = this.synchronized {

    if(!isStarted.get())
      throw new IllegalStateException(s"Consumer ${name} is not started. Start it first.")

    if(!getPartitions().contains(partition))
      throw new IllegalStateException(s"Consumer doesn't work on partition=${partition}.")

    if (transactionBuffer(partition).isEmpty) {
      transactionBuffer(partition) = stream.metadataStorage.commitEntity.getTransactions(
        streamName      = stream.getName,
        partition       = partition,
        fromTransaction = currentOffsets(partition),
        cnt             = options.transactionsPreload)

      if (transactionBuffer(partition).isEmpty) {
        return None
      }
    }

    val txn = transactionBuffer(partition).head

    if (txn.getCount() != -1) {
      updateOffsets(partition, txn.getTransactionUUID())
      transactionBuffer(partition).dequeue()
      txn.attach(this)
      return Some(txn)
    }

    val txnUpdatedOpt = getTransactionById(partition, txn.getTransactionUUID())
    if(txnUpdatedOpt.isDefined) {
      updateOffsets(partition, txnUpdatedOpt.get.getTransactionUUID())
      transactionBuffer(partition).dequeue()
      return txnUpdatedOpt
    }

    return None
  }

  private def updateOffsets(partition: Int, uuid: UUID) = {
    checkpointOffsets(partition)    = uuid
    currentOffsets(partition)       = uuid
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

    var uuid = options.txnGenerator.getTimeUUID()
    var isFinished = false
    while (!isFinished) {

      val queue = stream.metadataStorage.commitEntity.getLastTransactionHelper[T](stream.getName, partition, uuid)

      if (queue.isEmpty)
        isFinished = true
      else {
        while (queue.nonEmpty) {
          val txn: Transaction[T] = queue.dequeue()
          if (txn.getCount() != -1) {
            txn.attach(this)
            return Option[Transaction[T]](txn)
          }
          uuid = txn.getTransactionUUID()
        }
      }
    }
    None
  }

  def getTransactionsFromTo(partition: Int, from: UUID, to: UUID): ListBuffer[Transaction[T]] = {
    val comparator = new UUIDComparator()
    val txns = stream.metadataStorage.commitEntity.getTransactions[T](
                                          streamName = stream.getName,
                                          partition = partition,
                                          fromTransaction = from,
                                          cnt = options.transactionsPreload)
    val okList = ListBuffer[Transaction[T]]()
    var addFlag = true
    var moreItems = true
    while(addFlag && moreItems) {
      if(txns.isEmpty) {
        moreItems = false
      } else {
        val t = txns.dequeue()
        if(comparator.compare(to, t.getTransactionUUID()) > -1) {
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
    if(okList.size > 0 && addFlag && comparator.compare(to, okList.last.getTransactionUUID()) == 1)
      okList.appendAll(if (okList.size > 0) getTransactionsFromTo(partition, okList.last.getTransactionUUID(), to) else ListBuffer[Transaction[T]]())

    okList
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

    val txnOpt = loadTransactionFromDB(partition, uuid)
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
    * @param txn Transaction to update
    * @return Updated transaction
    */
  def loadTransactionFromDB(partition: Int, txn: UUID): Option[Transaction[T]] = this.synchronized {
    if(!isStarted.get())
      throw new IllegalStateException("Consumer is not started. Start consumer first.")

    val data: Option[(Int, Int)] =
      stream.metadataStorage.commitEntity.getTransactionItemCountAndTTL(stream.getName, partition, txn)

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
    stream.metadataStorage.consumerEntity.saveBatchOffset(name, stream.getName, checkpointOffsets)
    checkpointOffsets.clear()
  }

  /**
    * Info to commit
    */
  override def getCheckpointInfoAndClear(): List[CheckpointInfo] = this.synchronized {

    if(!isStarted.get())
      throw new IllegalStateException("Consumer is not started. Start consumer first.")

    val checkpointData = checkpointOffsets.map { case (partition, lastTxn) =>
      ConsumerCheckpointInfo(name, stream.getName, partition, lastTxn)
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
    * @param partition
    * @param uuid
    * @param count
    * @return
    */
  def buildTransactionObject(partition: Int, uuid: UUID, count: Int): Option[Transaction[T]] = {
    Subscriber.logger.info("1=1")
    val txn = new Transaction[T](partition, uuid, count, -1)
    Subscriber.logger.info("1=2")
    txn.attach(this)
    Subscriber.logger.info("1=3")
    Some(txn)
  }
}