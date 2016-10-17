package com.bwsw.tstreams.agents.producer

import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.group.ProducerCheckpointInfo
import com.bwsw.tstreams.common.LockUtil
import com.bwsw.tstreams.coordination.messages.state.{TransactionStateMessage, TransactionStatus}
import com.bwsw.tstreams.debug.GlobalHooks
import com.bwsw.tstreams.metadata.TransactionRecord
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

object ProducerTransaction {
  val logger = LoggerFactory.getLogger(this.getClass)
}

/**
  * Transaction retrieved by BasicProducer.newTransaction method
  *
  * @param partition        Concrete partition for saving this transaction
  * @param transactionOwner Producer class which was invoked newTransaction method
  * @param transactionID  ID for this transaction
  * @tparam T User data type
  */
class ProducerTransaction[T](partition: Int,
                             transactionID: Long,
                             transactionOwner: Producer[T]) extends IProducerTransaction[T] {

  private val transactionLock = new ReentrantLock()

  private val data = new ProducerTransactionData[T](this, transactionOwner.stream.ttl, transactionOwner.stream.dataStorage)

  /**
    * state of transaction
    */
  private val state = new ProducerTransactionState

  /**
    * State indicator of the transaction
    *
    * @return Closed transaction or not
    */
  def isClosed() = state.isClosed

  /**
    * BasicProducerTransaction logger for logging
    */
  ProducerTransaction.logger.debug(s"Open transaction $getTransactionID for\nstream, partition: ${transactionOwner.stream.name}, ${}")

  /**
    *
    */
  def markAsClosed() = state.closeOrDie

  /**
    * Return transaction partition
    */
  def getPartition(): Int = partition

  /**
    * makes transaction materialized
    */
  def makeMaterialized(): Unit = {
    state.makeMaterialized()
  }

  override def toString(): String = s"producer.Transaction(ID=$transactionID, partition=$partition, count=${getDataItemsCount()})"

  def awaitMaterialized(): Unit = {
    if (ProducerTransaction.logger.isDebugEnabled) {
      ProducerTransaction.logger.debug(s"Await for transaction $getTransactionID to be materialized\nfor stream,partition : ${transactionOwner.stream.name},$partition")
    }
    state.awaitMaterialization(transactionOwner.producerOptions.coordinationOptions.transport.getTimeout())
    if (ProducerTransaction.logger.isDebugEnabled) {
      ProducerTransaction.logger.debug(s"Transaction $getTransactionID is materialized\nfor stream,partition : ${transactionOwner.stream.name}, $partition")
    }
  }

  /**
    * Return transaction ID
    */
  def getTransactionID(): Long = transactionID

  /**
    * Return current transaction amount of data
    */
  def getDataItemsCount() = data.lastOffset

  /**
    * Returns Transaction owner
    */
  def getTransactionOwner() = transactionOwner

  /**
    * All inserts (can be async) in storage (must be waited before closing this transaction)
    */
  private var jobs = ListBuffer[() => Unit]()


  /**
    * Send data to storage
    *
    * @param obj some user object
    */
  def send(obj: T): Unit = {
    state.isOpenedOrDie()
    val number = data.put(obj, transactionOwner.producerOptions.converter)
    val job = {
      if (number % transactionOwner.producerOptions.batchSize == 0) {
        data.save()
      }
      else {
        null
      }
    }
    if (job != null) jobs += job
  }

  /**
    * Does actual send of the data that is not sent yet
    */
  def finalizeDataSend(): Unit = {
    val job = data.save()
    if (job != null) jobs += job
  }

  private def cancelAsync() = {
    transactionOwner.tsdb.del(partition, transactionID)
    val msg = TransactionStateMessage(transactionID = transactionID,
      ttl = -1,
      status = TransactionStatus.cancel,
      partition = partition,
      masterID = transactionOwner.p2pAgent.getUniqueAgentID(),
      orderID = -1,
      count = 0)
    transactionOwner.p2pAgent.publish(msg)
  }

  /**
    * Canceling current transaction
    */
  def cancel(): Unit = {
    state.isOpenedOrDie()
    state.awaitMaterialization(transactionOwner.producerOptions.coordinationOptions.transport.getTimeout())
    LockUtil.withLockOrDieDo[Unit](transactionLock, (100, TimeUnit.SECONDS), Some(ProducerTransaction.logger), () => {
      state.awaitUpdateComplete
      state.closeOrDie
      transactionOwner.asyncActivityService.submit("<CancelAsyncTask>", new Runnable {
        override def run(): Unit = cancelAsync()
      }, Option(transactionLock))
    })
  }

  private def checkpointPostEventPart(): Unit = {
    if (ProducerTransaction.logger.isDebugEnabled) {
      ProducerTransaction.logger.debug(s"[COMMIT PARTITION_{}] ts={}", partition, transactionID.toString)
    }

    //debug purposes only
    {
      val interruptExecution: Boolean = try {
        GlobalHooks.invoke(GlobalHooks.afterCommitFailure)
        false
      } catch {
        case e: Exception =>
          ProducerTransaction.logger.warn("AfterCommitFailure in DEBUG mode")
          true
      }
      if (interruptExecution)
        return
    }

    transactionOwner.p2pAgent.publish(TransactionStateMessage(
      transactionID = transactionID,
      ttl = -1,
      status = TransactionStatus.checkpointed,
      partition = partition,
      masterID = transactionOwner.getPartitionMasterIDLocalInfo(partition),
      orderID = -1,
      count = getDataItemsCount()
    ))

    if (ProducerTransaction.logger.isDebugEnabled) {
      ProducerTransaction.logger.debug("[FINAL CHECKPOINT PARTITION_{}] ts={}", partition, transactionID.toString)
    }
  }


  private def checkpointAsync(): Unit = {
    finalizeDataSend()
    //close transaction using stream ttl
    if (getDataItemsCount > 0) {
      jobs.foreach(x => x())

      if (ProducerTransaction.logger.isDebugEnabled) {
        ProducerTransaction.logger.debug("[START PRE CHECKPOINT PARTITION_{}] ts={}", partition, transactionID.toString)
      }

      //debug purposes only
      {
        val interruptExecution: Boolean = try {
          GlobalHooks.invoke(GlobalHooks.preCommitFailure)
          false
        } catch {
          case e: Exception =>
            ProducerTransaction.logger.warn("PreCommitFailure in DEBUG mode")
            true
        }
        if (interruptExecution) {
          return
        }
      }
      val record = TransactionRecord(partition = partition, transactionID = transactionID, count = getDataItemsCount(), ttl = transactionOwner.stream.ttl)
      transactionOwner.tsdb.put(record, transactionOwner.backendActivityService) (record => { checkpointPostEventPart() })
    }
    else {
      transactionOwner.p2pAgent.publish(TransactionStateMessage(
        transactionID = transactionID,
        ttl = -1,
        status = TransactionStatus.cancel,
        partition = partition,
        masterID = transactionOwner.p2pAgent.getUniqueAgentID(),
        orderID = -1, count = 0))
    }
  }

  /**
    * Submit transaction(transaction will be available by consumer only after closing)
    */
  def checkpoint(isSynchronous: Boolean = true): Unit = {
    state.isOpenedOrDie()
    state.awaitMaterialization(transactionOwner.producerOptions.coordinationOptions.transport.getTimeout())
    LockUtil.withLockOrDieDo[Unit](transactionLock, (100, TimeUnit.SECONDS), Some(ProducerTransaction.logger), () => {
      state.awaitUpdateComplete()
      state.closeOrDie()
      if (!isSynchronous) {
        transactionOwner.asyncActivityService.submit("<CheckpointAsyncTask>", new Runnable {
          override def run(): Unit = checkpointAsync()
        }, Option(transactionLock))
      }
      else {
        finalizeDataSend()

        if (getDataItemsCount > 0) {
          jobs.foreach(x => x())

          if (ProducerTransaction.logger.isDebugEnabled) {
            ProducerTransaction.logger.debug("[START PRE CHECKPOINT PARTITION_{}] ts={}", partition, transactionID.toString)
          }

          //debug purposes only
          GlobalHooks.invoke(GlobalHooks.preCommitFailure)

          val latch = new CountDownLatch(1)
          val record = TransactionRecord(partition = partition, transactionID = transactionID, count = getDataItemsCount(), ttl = transactionOwner.stream.ttl)
          transactionOwner.tsdb.put(record, transactionOwner.backendActivityService) (record => { latch.countDown() })
          latch.await()

          if (ProducerTransaction.logger.isDebugEnabled) {
            ProducerTransaction.logger.debug("[COMMIT PARTITION_{}] ts={}", partition, transactionID.toString)
          }
          //debug purposes only
          GlobalHooks.invoke(GlobalHooks.afterCommitFailure)

          transactionOwner.p2pAgent.publish(TransactionStateMessage(
            transactionID = transactionID,
            ttl = -1,
            status = TransactionStatus.checkpointed,
            partition = partition,
            masterID = transactionOwner.getPartitionMasterIDLocalInfo(partition),
            orderID = -1,
            count = getDataItemsCount()))

          if (ProducerTransaction.logger.isDebugEnabled) {
            ProducerTransaction.logger.debug("[FINAL CHECKPOINT PARTITION_{}] ts={}", partition, transactionID.toString)
          }
        }
        else {
          transactionOwner.p2pAgent.publish(TransactionStateMessage(
            transactionID = transactionID,
            ttl = -1,
            status = TransactionStatus.cancel,
            partition = partition,
            masterID = transactionOwner.p2pAgent.getUniqueAgentID(),
            orderID = -1,
            count = 0))
        }
      }
    })
  }

  private def doSendUpdateMessage() = {
    {
      GlobalHooks.invoke(GlobalHooks.transactionUpdateTaskEnd)
    }

    state.setUpdateFinished
    transactionOwner.p2pAgent.publish(TransactionStateMessage(
      transactionID = transactionID,
      ttl = transactionOwner.producerOptions.transactionTTL,
      status = TransactionStatus.update,
      partition = partition,
      masterID = transactionOwner.p2pAgent.getUniqueAgentID(),
      orderID = -1,
      count = 0))

    if (ProducerTransaction.logger.isDebugEnabled) {
      ProducerTransaction.logger.debug(s"[KEEP_ALIVE THREAD PARTITION_PARTITION_$partition] ts=${transactionID.toString} status=${TransactionStatus.update}")
    }
  }

  def updateTransactionKeepAliveState(): Unit = {
    if (!state.isMaterialized())
      return
    // atomically check state and launch update process
    val stateOnUpdateClosed =
      LockUtil.withLockOrDieDo[Boolean](transactionLock, (100, TimeUnit.SECONDS), Some(ProducerTransaction.logger), () => {
        val s = state.isClosed()
        if (!s) state.setUpdateInProgress()
        s
      })

    // if atomic state was closed then update process should be aborted
    // immediately
    if (stateOnUpdateClosed)
      return

    {
      GlobalHooks.invoke(GlobalHooks.transactionUpdateTaskBegin)
    }

    //-1 here indicate that transaction is started but is not finished yet
    if (ProducerTransaction.logger.isDebugEnabled) {
      ProducerTransaction.logger.debug("Update event for Transaction {}, partition: {}", transactionID, partition)
    }

    val record = TransactionRecord(partition = partition, transactionID = transactionID, count = -1, ttl = transactionOwner.stream.ttl)
    transactionOwner.tsdb.put(record, transactionOwner.backendActivityService) (record => { doSendUpdateMessage() })

  }

  /**
    * accessor to lock object for external agents
    *
    * @return
    */
  def getTransactionLock(): ReentrantLock = transactionLock


  def getTransactionInfo(): ProducerCheckpointInfo = {
    state.awaitMaterialization(transactionOwner.producerOptions.coordinationOptions.transport.getTimeout())

    val checkpoint = TransactionStateMessage(
      transactionID = getTransactionID(),
      ttl = -1,
      status = TransactionStatus.checkpointed,
      partition = partition,
      masterID = transactionOwner.getPartitionMasterIDLocalInfo(partition),
      orderID = -1,
      count = getDataItemsCount())

    ProducerCheckpointInfo(transactionRef = this,
      agent = transactionOwner.p2pAgent,
      checkpointEvent = checkpoint,
      streamName = transactionOwner.stream.name,
      partition = partition,
      transaction = getTransactionID(),
      totalCnt = getDataItemsCount(),
      ttl = transactionOwner.stream.ttl)
  }
}