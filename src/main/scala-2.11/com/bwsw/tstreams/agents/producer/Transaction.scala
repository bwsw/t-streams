package com.bwsw.tstreams.agents.producer

import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.agents.group.ProducerCheckpointInfo
import com.bwsw.tstreams.common.LockUtil
import com.bwsw.tstreams.coordination.messages.state.{Message, TransactionStatus}
import com.bwsw.tstreams.debug.GlobalHooks
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

/**
  * Transaction retrieved by BasicProducer.newTransaction method
  *
  * @param transactionLock Transaction Lock for managing actions which has to do with checkpoints
  * @param partition       Concrete partition for saving this transaction
  * @param txnOwner        Producer class which was invoked newTransaction method
  * @param transactionUuid UUID for this transaction
  * @tparam USERTYPE User data type
  */
class Transaction[USERTYPE](transactionLock: ReentrantLock,
                            partition: Int,
                            transactionUuid: UUID,
                            txnOwner: Producer[USERTYPE]) {



  /**
    * state of transaction
    */
  private val state = new TransactionState

  /**
    * State indicator of the transaction
    *
    * @return Closed transaction or not
    */
  def isClosed = state.isClosed

  /**
    * BasicProducerTransaction logger for logging
    */
  private val logger = LoggerFactory.getLogger(this.getClass)
  logger.debug(s"\nOpen transaction ${getTxnUUID} for\nstream, partition: {${txnOwner.stream.getName}}, {$partition}")

  /**
    *
    */
  def markAsClosed() = state.closeOrDie

  /**
    * Return transaction partition
    */
  def getPartition: Int = partition

  /**
    * makes transaction materialized
    */
  def makeMaterialized(): Unit = {
    logger.debug(s"Materialize transaction ${getTxnUUID} for\nstream,partition : {${txnOwner.stream.getName}},{$partition}")
    state.makeMaterialized()
  }

  def awaitMaterialized(): Unit = {
    logger.debug(s"Await for transaction ${getTxnUUID} to be materialized\nfor stream,partition : {${txnOwner.stream.getName}},{$partition}")
    state.awaitMaterialization(txnOwner.producerOptions.coordinationOptions.transport.getTimeout())
    logger.debug(s"Transaction ${getTxnUUID} is materialized\nfor stream,partition : {${txnOwner.stream.getName}},{$partition}")
  }

  /**
    * Return transaction UUID
    */
  def getTxnUUID: UUID = transactionUuid

  /**
    * Return current transaction amount of data
    */
  def getCnt = part.get

  /**
    * Transaction part index
    */
  private var part = new AtomicInteger(0)

  /**
    * All inserts (can be async) in storage (must be waited before closing this transaction)
    */
  private var jobs = ListBuffer[() => Unit]()


  /**
    * Send data to storage
    *
    * @param obj some user object
    */
  def send(obj: USERTYPE): Unit = {
    state.isOpenedOrDie

    LockUtil.withLockOrDieDo[Unit](transactionLock, (100, TimeUnit.SECONDS), Some(logger), () => {
      txnOwner.producerOptions.insertType match {

        case DataInsertType.BatchInsert(size) =>

          txnOwner.stream.dataStorage.putInBuffer(
            txnOwner.stream.getName,
            partition,
            transactionUuid,
            txnOwner.stream.getTTL,
            txnOwner.producerOptions.converter.convert(obj),
            part.get)

          if (txnOwner.stream.dataStorage.getBufferSize(transactionUuid) == size) {

            val job: () => Unit = txnOwner.stream.dataStorage.saveBuffer(transactionUuid)
            if (job != null) jobs += job
            txnOwner.stream.dataStorage.clearBuffer(transactionUuid)

          }

        case DataInsertType.SingleElementInsert =>

          val job: () => Unit = txnOwner.stream.dataStorage.put(
            txnOwner.stream.getName,
            partition,
            transactionUuid,
            txnOwner.stream.getTTL,
            txnOwner.producerOptions.converter.convert(obj),
            part.get)
          if (job != null) jobs += job
      }

      part.incrementAndGet() })
  }


  private def cancelAsync() = {
    txnOwner.producerOptions.insertType match {
      case DataInsertType.SingleElementInsert =>

      case DataInsertType.BatchInsert(_) =>
        txnOwner.stream.dataStorage.clearBuffer(transactionUuid)
    }

    txnOwner.stream.metadataStorage.commitEntity.deleteAsync(
      streamName  = txnOwner.stream.getName,
      partition   = partition,
      transaction = transactionUuid,
      executor    = txnOwner.backendActivityService,
      function    = () => {
        val msg = Message(txnUuid = transactionUuid,
          ttl = -1,
          status = TransactionStatus.cancel,
          partition = partition)
        txnOwner.p2pAgent.publish(msg)
        logger.debug(s"[CANCEL PARTITION_${msg.partition}] ts=${msg.txnUuid.timestamp()} status=${msg.status}")
      })
  }

  /**
    * Canceling current transaction
    */
  def cancel() = {
    state.awaitMaterialization(txnOwner.producerOptions.coordinationOptions.transport.getTimeout())
    LockUtil.withLockOrDieDo[Unit](transactionLock, (100, TimeUnit.SECONDS), Some(logger), () => {
      state.awaitUpdateComplete
      state.closeOrDie
      txnOwner.backendActivityService.submit(new Runnable {
        override def run(): Unit = cancelAsync()
      }, Option(transactionLock))
    })
  }

  private def checkpointPostEventPart() : Unit = {
    logger.debug(s"[COMMIT PARTITION_$partition] ts=${transactionUuid.timestamp()}")

    //debug purposes only
    {
      val interruptExecution: Boolean = try {
        GlobalHooks.invoke(GlobalHooks.afterCommitFailure)
        false
      } catch {
        case e: Exception =>
          logger.warn("AfterCommitFailure in DEBUG mode")
          true
      }
      if (interruptExecution)
        return
    }

    txnOwner.p2pAgent.publish(Message(
      txnUuid = transactionUuid,
      ttl = -1,
      status = TransactionStatus.postCheckpoint,
      partition = partition))

    logger.debug(s"[FINAL CHECKPOINT PARTITION_$partition] " +
      s"ts=${transactionUuid.timestamp()}")
  }


  private def checkpointAsync() : Unit = {
    txnOwner.producerOptions.insertType match {

      case DataInsertType.SingleElementInsert =>

      case DataInsertType.BatchInsert(size) =>
        if (txnOwner.stream.dataStorage.getBufferSize(transactionUuid) > 0) {
          val job: () => Unit = txnOwner.stream.dataStorage.saveBuffer(transactionUuid)
          if (job != null) jobs += job
          txnOwner.stream.dataStorage.clearBuffer(transactionUuid)
        }
    }
    //close transaction using stream ttl
    if (part.get() > 0) {
      jobs.foreach(x => x())

      logger.debug(s"[START PRE CHECKPOINT PARTITION_$partition] " +
        s"ts=${transactionUuid.timestamp()}")

      txnOwner.p2pAgent.publish(Message(
        txnUuid = transactionUuid,
        ttl = -1,
        status = TransactionStatus.preCheckpoint,
        partition = partition))

      //debug purposes only
      {
        val interruptExecution: Boolean = try {
          GlobalHooks.invoke(GlobalHooks.preCommitFailure)
          false
        } catch {
          case e: Exception =>
            logger.warn("PreCommitFailure in DEBUG mode")
            true
        }
        if (interruptExecution) {
          return
        }
      }

      txnOwner.stream.metadataStorage.commitEntity.commitAsync(
        streamName = txnOwner.stream.getName,
        partition = partition,
        transaction = transactionUuid,
        totalCnt = part.get(),
        ttl = txnOwner.stream.getTTL,
        executor = txnOwner.backendActivityService,
        function = checkpointPostEventPart)
    }
    else {
      txnOwner.p2pAgent.publish(Message(
        txnUuid = transactionUuid,
        ttl = -1,
        status = TransactionStatus.cancel,
        partition = partition))
    }
  }

  /**
    * Submit transaction(transaction will be available by consumer only after closing)
    */
  def checkpoint(isSynchronous: Boolean = true): Unit = {
    state.awaitMaterialization(txnOwner.producerOptions.coordinationOptions.transport.getTimeout())
    LockUtil.withLockOrDieDo[Unit](transactionLock, (100, TimeUnit.SECONDS), Some(logger), () => {
      state.awaitUpdateComplete
      state.closeOrDie
      if (!isSynchronous) {
        txnOwner.backendActivityService.submit(new Runnable {
          override def run(): Unit = checkpointAsync()
        }, Option(transactionLock))
      }
      else {
        txnOwner.producerOptions.insertType match {

          case DataInsertType.SingleElementInsert =>

          case DataInsertType.BatchInsert(size) =>
            if (txnOwner.stream.dataStorage.getBufferSize(transactionUuid) > 0) {
              val job: () => Unit = txnOwner.stream.dataStorage.saveBuffer(transactionUuid)
              if (job != null) jobs += job
              txnOwner.stream.dataStorage.clearBuffer(transactionUuid)
            }
        }

        if (part.get() > 0) {
          jobs.foreach(x => x())

          logger.debug(s"[START PRE CHECKPOINT PARTITION_$partition] " +
            s"ts=${transactionUuid.timestamp()}")

          txnOwner.p2pAgent.publish(Message(
            txnUuid = transactionUuid,
            ttl = -1,
            status = TransactionStatus.preCheckpoint,
            partition = partition))

          //debug purposes only
          GlobalHooks.invoke(GlobalHooks.preCommitFailure)

          txnOwner.stream.metadataStorage.commitEntity.commit(
            streamName = txnOwner.stream.getName,
            partition = partition,
            transaction = transactionUuid,
            totalCnt = part.get(),
            ttl = txnOwner.stream.getTTL)

          logger.debug(s"[COMMIT PARTITION_$partition] ts=${transactionUuid.timestamp()}")

          //debug purposes only
          GlobalHooks.invoke(GlobalHooks.afterCommitFailure)

          txnOwner.p2pAgent.publish(Message(
            txnUuid = transactionUuid,
            ttl = -1,
            status = TransactionStatus.postCheckpoint,
            partition = partition))

          logger.debug(s"[FINAL CHECKPOINT PARTITION_$partition] " +
            s"ts=${transactionUuid.timestamp()}")

        }
        else {
          txnOwner.p2pAgent.publish(Message(
            txnUuid = transactionUuid,
            ttl = -1,
            status = TransactionStatus.cancel,
            partition = partition))
        }
      }
    })
  }

  private def doSendUpdateMessage() = {
    //publish that current txn is being updating

    {
      GlobalHooks.invoke(GlobalHooks.transactionUpdateTaskEnd)
    }

    state.setUpdateFinished
    txnOwner.p2pAgent.publish(Message(
      txnUuid = transactionUuid,
      ttl = txnOwner.producerOptions.transactionTTL,
      status = TransactionStatus.update,
      partition = partition))
    logger.debug(s"[KEEP_ALIVE THREAD PARTITION_${partition}] ts=${transactionUuid.timestamp()} status=${TransactionStatus.update}")
  }

  def updateTxnKeepAliveState(): Unit = {
    if(!state.isMaterialized)
      return
    // atomically check state and launch update process
    val stateOnUpdateClosed =
      LockUtil.withLockOrDieDo[Boolean](transactionLock, (100, TimeUnit.SECONDS), Some(logger), () => {
        val s = state.isClosed
        if (!s) state.setUpdateInProgress
        s })

    // if atomic state was closed then update process should be aborted
    // immediately
    if(stateOnUpdateClosed)
      return

    {
      GlobalHooks.invoke(GlobalHooks.transactionUpdateTaskBegin)
    }

    //-1 here indicate that transaction is started but is not finished yet
    logger.debug(s"Update event for txn ${transactionUuid}, partition: ${partition}")
    val f = txnOwner.stream.metadataStorage.commitEntity.commitAsync(
    streamName = txnOwner.stream.getName,
    partition = partition,
    transaction = transactionUuid,
    totalCnt = -1,
    ttl = txnOwner.producerOptions.transactionTTL,
    executor = txnOwner.backendActivityService,
    function = doSendUpdateMessage)
  }

  /**
    * accessor to lock object for external agents
    *
    * @return
    */
  def getTransactionLock(): ReentrantLock = transactionLock


  def getTransactionInfo(): ProducerCheckpointInfo = {
    state.awaitMaterialization(txnOwner.producerOptions.coordinationOptions.transport.getTimeout())

    val preCheckpoint = Message(
      txnUuid = getTxnUUID,
      ttl = -1,
      status = TransactionStatus.preCheckpoint,
      partition = partition)

    val finalCheckpoint = Message(
      txnUuid = getTxnUUID,
      ttl = -1,
      status = TransactionStatus.postCheckpoint,
      partition = partition)

    ProducerCheckpointInfo(transactionRef = this,
      agent = txnOwner.p2pAgent,
      preCheckpointEvent = preCheckpoint,
      finalCheckpointEvent = finalCheckpoint,
      streamName = txnOwner.stream.getName,
      partition = partition,
      transaction = getTxnUUID,
      totalCnt = getCnt,
      ttl = txnOwner.stream.getTTL)
  }
}