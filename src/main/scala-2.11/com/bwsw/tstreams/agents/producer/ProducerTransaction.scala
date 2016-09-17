package com.bwsw.tstreams.agents.producer

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.agents.group.ProducerCheckpointInfo
import com.bwsw.tstreams.common.LockUtil
import com.bwsw.tstreams.coordination.messages.state.{TransactionStateMessage, TransactionStatus}
import com.bwsw.tstreams.debug.GlobalHooks
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

  private val data = new ProducerTransactionData[T](this, transactionOwner.stream.getTTL, transactionOwner.stream.dataStorage)

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
  ProducerTransaction.logger.debug(s"Open transaction $getTransactionID for\nstream, partition: ${transactionOwner.stream.getName}, ${}")

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
      ProducerTransaction.logger.debug(s"Await for transaction $getTransactionID to be materialized\nfor stream,partition : ${transactionOwner.stream.getName},$partition")
    }
    state.awaitMaterialization(transactionOwner.producerOptions.coordinationOptions.transport.getTimeout())
    if (ProducerTransaction.logger.isDebugEnabled) {
      ProducerTransaction.logger.debug(s"Transaction $getTransactionID is materialized\nfor stream,partition : ${transactionOwner.stream.getName}, $partition")
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
    transactionOwner.stream.metadataStorage.commitEntity.deleteAsync(
      streamName = transactionOwner.stream.getName,
      partition = partition,
      transaction = transactionID,
      executor = transactionOwner.backendActivityService,
      resourceCounter = transactionOwner.pendingCassandraTasks,
      function = () => {
        val msg = TransactionStateMessage(transactionID = transactionID,
          ttl = -1,
          status = TransactionStatus.cancel,
          partition = partition,
          masterID = transactionOwner.p2pAgent.getUniqueAgentID(),
          orderID = -1,
          count = 0)
        transactionOwner.p2pAgent.publish(msg)
        if (ProducerTransaction.logger.isDebugEnabled) {
          ProducerTransaction.logger.debug(s"[CANCEL PARTITION_${msg.partition}] ts=${msg.transactionID.toString} status=${msg.status.toString}")
        }
      })
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
      status = TransactionStatus.postCheckpoint,
      partition = partition,
      masterID = transactionOwner.getPartitionMasterID(partition),
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

      transactionOwner.p2pAgent.publish(TransactionStateMessage(
        transactionID = transactionID,
        ttl = -1,
        status = TransactionStatus.preCheckpoint,
        partition = partition,
        masterID = transactionOwner.getPartitionMasterID(partition),
        orderID = -1,
        count = getDataItemsCount()))

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

      transactionOwner.stream.metadataStorage.commitEntity.commitAsync(
        streamName = transactionOwner.stream.getName,
        partition = partition,
        transaction = transactionID,
        totalCnt = getDataItemsCount,
        ttl = transactionOwner.stream.getTTL,
        resourceCounter = transactionOwner.pendingCassandraTasks,
        executor = transactionOwner.backendActivityService,
        function = checkpointPostEventPart)

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

          transactionOwner.p2pAgent.publish(TransactionStateMessage(
            transactionID = transactionID,
            ttl = -1,
            status = TransactionStatus.preCheckpoint,
            partition = partition,
            masterID = transactionOwner.getPartitionMasterID(partition),
            orderID = -1,
            count = getDataItemsCount()))

          //debug purposes only
          GlobalHooks.invoke(GlobalHooks.preCommitFailure)

          transactionOwner.stream.metadataStorage.commitEntity.commit(
            streamName = transactionOwner.stream.getName,
            partition = partition,
            transaction = transactionID,
            totalCnt = getDataItemsCount,
            ttl = transactionOwner.stream.getTTL)

          if (ProducerTransaction.logger.isDebugEnabled) {
            ProducerTransaction.logger.debug("[COMMIT PARTITION_{}] ts={}", partition, transactionID.toString)
          }
          //debug purposes only
          GlobalHooks.invoke(GlobalHooks.afterCommitFailure)

          transactionOwner.p2pAgent.publish(TransactionStateMessage(
            transactionID = transactionID,
            ttl = -1,
            status = TransactionStatus.postCheckpoint,
            partition = partition,
            masterID = transactionOwner.getPartitionMasterID(partition),
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

    transactionOwner.stream.metadataStorage.commitEntity.commitAsync(
      streamName = transactionOwner.stream.getName,
      partition = partition,
      transaction = transactionID,
      totalCnt = -1,
      resourceCounter = transactionOwner.pendingCassandraTasks,
      ttl = transactionOwner.producerOptions.transactionTTL,
      executor = transactionOwner.backendActivityService,
      function = doSendUpdateMessage)

  }

  /**
    * accessor to lock object for external agents
    *
    * @return
    */
  def getTransactionLock(): ReentrantLock = transactionLock


  def getTransactionInfo(): ProducerCheckpointInfo = {
    state.awaitMaterialization(transactionOwner.producerOptions.coordinationOptions.transport.getTimeout())

    val preCheckpoint = TransactionStateMessage(
      transactionID = getTransactionID,
      ttl = -1,
      status = TransactionStatus.preCheckpoint,
      partition = partition,
      masterID = transactionOwner.getPartitionMasterID(partition),
      orderID = -1,
      count = getDataItemsCount())

    val postCheckpoint = TransactionStateMessage(
      transactionID = getTransactionID(),
      ttl = -1,
      status = TransactionStatus.postCheckpoint,
      partition = partition,
      masterID = transactionOwner.getPartitionMasterID(partition),
      orderID = -1,
      count = getDataItemsCount())

    ProducerCheckpointInfo(transactionRef = this,
      agent = transactionOwner.p2pAgent,
      preCheckpointEvent = preCheckpoint,
      postCheckpointEvent = postCheckpoint,
      streamName = transactionOwner.stream.getName,
      partition = partition,
      transaction = getTransactionID(),
      totalCnt = getDataItemsCount(),
      ttl = transactionOwner.stream.getTTL)
  }
}