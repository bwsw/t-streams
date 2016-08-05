package com.bwsw.tstreams.agents.producer

import java.util.UUID
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import com.bwsw.ResettableCountDownLatch
import com.bwsw.tstreams.common.LockUtil
import com.bwsw.tstreams.coordination.pubsub.messages.{ProducerTopicMessage, ProducerTransactionStatus}
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
class ProducerTransaction[USERTYPE](transactionLock: ReentrantLock,
                                    partition: Int,
                                    transactionUuid: UUID,
                                    txnOwner: Producer[USERTYPE]) {

  /**
    * This value is used to optimize New Transaction with latch
    */
  val startTime = System.currentTimeMillis()

  /**
    * This latch is used to await when master will materialize the Transaction.
    * Before materialization complete checkpoints, updates, cancels are not permitted.
    */
  val materialize = new CountDownLatch(1)

  /**
    * This atomic used to make update exit if Transaction is not materialized
    */
  val isMaterialized = new AtomicBoolean(false)


  /**
    * State indicator of the transaction
    *
    * @return Closed transaction or not
    */
  def isClosed = closed.get

  /**
    * BasicProducerTransaction logger for logging
    */
  private val logger = LoggerFactory.getLogger(this.getClass)
  logger.debug(s"Open transaction for stream,partition : {${txnOwner.stream.getName}},{$partition}")

  /**
    *
    */
  def setAsClosed() =
    closed.set(true)

  /**
    * Return transaction partition
    */
  def getPartition: Int = partition

  /**
    * Return transaction UUID
    */
  def getTxnUUID: UUID = transactionUuid

  /**
    * Return current transaction amount of data
    */
  def getCnt = part

  /**
    * Variable for indicating transaction state
    */
  private val closed = new AtomicBoolean(false)

  /**
    * Transaction part index
    */
  private var part = 0

  /**
    * All inserts (can be async) in storage (must be waited before closing this transaction)
    */
  private var jobs = ListBuffer[() => Unit]()

  /**
    * This special trigger is used to avoid a very specific race, which could happen if
    * checkpoint/cancel will be called same time when update will do update. So, we actully
    * must protect from this situation, that's why during update, latch must be set to make
    * cancel/checkpoint wait until update will complete
    * We also need special test for it.
    */
  private val updateSignalVar = new ResettableCountDownLatch(0)


  /**
    * Send data to storage
    *
    * @param obj some user object
    */
  def send(obj: USERTYPE): Unit = {
    if (closed.get)
      throw new IllegalStateException("transaction is closed")

    LockUtil.withLockOrDieDo[Unit](transactionLock, (100, TimeUnit.SECONDS), Some(logger), () => {
      txnOwner.producerOptions.insertType match {

        case DataInsertType.BatchInsert(size) =>

          txnOwner.stream.dataStorage.putInBuffer(
            txnOwner.stream.getName,
            partition,
            transactionUuid,
            txnOwner.stream.getTTL,
            txnOwner.producerOptions.converter.convert(obj),
            part)

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
            part)
          if (job != null) jobs += job
      }

      part += 1 })
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
        val msg = ProducerTopicMessage(txnUuid = transactionUuid,
          ttl = -1,
          status = ProducerTransactionStatus.cancel,
          partition = partition)
        txnOwner.masterP2PAgent.publish(msg)
        logger.debug(s"[CANCEL PARTITION_${msg.partition}] ts=${msg.txnUuid.timestamp()} status=${msg.status}")
      })
  }

  /**
    * Canceling current transaction
    */
  def cancel() = {
    if(!updateSignalVar.await(10, TimeUnit.SECONDS))
      throw new IllegalStateException("Update takes too long (> 10 seconds). Probably failure.")

    if (closed.getAndSet(true))
      throw new IllegalStateException("transaction is already closed")

    LockUtil.withLockOrDieDo[Unit](transactionLock, (100, TimeUnit.SECONDS), Some(logger), () => {
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

    txnOwner.masterP2PAgent.publish(ProducerTopicMessage(
      txnUuid = transactionUuid,
      ttl = -1,
      status = ProducerTransactionStatus.postCheckpoint,
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
    if (part > 0) {
      jobs.foreach(x => x())

      logger.debug(s"[START PRE CHECKPOINT PARTITION_$partition] " +
        s"ts=${transactionUuid.timestamp()}")

      txnOwner.masterP2PAgent.publish(ProducerTopicMessage(
        txnUuid = transactionUuid,
        ttl = -1,
        status = ProducerTransactionStatus.preCheckpoint,
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
        totalCnt = part,
        ttl = txnOwner.stream.getTTL,
        executor = txnOwner.backendActivityService,
        function = checkpointPostEventPart)
    }
    else {
      txnOwner.masterP2PAgent.publish(ProducerTopicMessage(
        txnUuid = transactionUuid,
        ttl = -1,
        status = ProducerTransactionStatus.cancel,
        partition = partition))
    }
  }

  /**
    * Submit transaction(transaction will be available by consumer only after closing)
    */
  def checkpoint(isSynchronous: Boolean = true): Unit = {
    if(!updateSignalVar.await(10, TimeUnit.SECONDS))
      throw new IllegalStateException("Update takes too long (> 10 seconds). Probably failure.")

    if (closed.getAndSet(true))
      throw new IllegalStateException("Transaction is closed already")

    LockUtil.withLockOrDieDo[Unit](transactionLock, (100, TimeUnit.SECONDS), Some(logger), () => {
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

        if (part > 0) {
          jobs.foreach(x => x())

          logger.debug(s"[START PRE CHECKPOINT PARTITION_$partition] " +
            s"ts=${transactionUuid.timestamp()}")

          txnOwner.masterP2PAgent.publish(ProducerTopicMessage(
            txnUuid = transactionUuid,
            ttl = -1,
            status = ProducerTransactionStatus.preCheckpoint,
            partition = partition))

          //debug purposes only
          GlobalHooks.invoke(GlobalHooks.preCommitFailure)

          txnOwner.stream.metadataStorage.commitEntity.commit(
            streamName = txnOwner.stream.getName,
            partition = partition,
            transaction = transactionUuid,
            totalCnt = part,
            ttl = txnOwner.stream.getTTL)

          logger.debug(s"[COMMIT PARTITION_$partition] ts=${transactionUuid.timestamp()}")

          //debug purposes only
          GlobalHooks.invoke(GlobalHooks.afterCommitFailure)

          txnOwner.masterP2PAgent.publish(ProducerTopicMessage(
            txnUuid = transactionUuid,
            ttl = -1,
            status = ProducerTransactionStatus.postCheckpoint,
            partition = partition))

          logger.debug(s"[FINAL CHECKPOINT PARTITION_$partition] " +
            s"ts=${transactionUuid.timestamp()}")

        }
        else {
          txnOwner.masterP2PAgent.publish(ProducerTopicMessage(
            txnUuid = transactionUuid,
            ttl = -1,
            status = ProducerTransactionStatus.cancel,
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

    updateSignalVar.countDown()
    txnOwner.subscriberNotifier.publish(ProducerTopicMessage(
      txnUuid = transactionUuid,
      ttl = txnOwner.producerOptions.transactionTTL,
      status = ProducerTransactionStatus.update,
      partition = partition), () => ())
    logger.debug(s"[KEEP_ALIVE THREAD PARTITION_${partition}] ts=${transactionUuid.timestamp()} status=${ProducerTransactionStatus.update}")
  }

  def updateTxnKeepAliveState(): Unit = {
    if(closed.get)
      return
    updateSignalVar.setValue(1)

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
}