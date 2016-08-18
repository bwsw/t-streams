package com.bwsw.tstreams.agents.producer

import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.agents.group.ProducerCheckpointInfo
import com.bwsw.tstreams.common.LockUtil
import com.bwsw.tstreams.coordination.messages.state.{Message, TransactionStatus}
import com.bwsw.tstreams.debug.GlobalHooks
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

object Transaction {
  val logger = LoggerFactory.getLogger(this.getClass)
}

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


  private val data = new TransactionData[USERTYPE](this, txnOwner.stream.getTTL, txnOwner.stream.dataStorage)

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
  Transaction.logger.debug(s"Open transaction ${getTransactionUUID} for\nstream, partition: ${txnOwner.stream.getName}, ${}")

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
    state.makeMaterialized()
  }

  override def toString(): String = s"producer.Transaction(uuid=${transactionUuid}, partition=${partition}, count=${getDataItemsCount()})"

  def awaitMaterialized(): Unit = {
    if(Transaction.logger.isDebugEnabled) {
      Transaction.logger.debug(s"Await for transaction ${getTransactionUUID} to be materialized\nfor stream,partition : ${txnOwner.stream.getName},${partition}")
    }
    state.awaitMaterialization(txnOwner.producerOptions.coordinationOptions.transport.getTimeout())
    if(Transaction.logger.isDebugEnabled) {
      Transaction.logger.debug(s"Transaction ${getTransactionUUID} is materialized\nfor stream,partition : ${txnOwner.stream.getName}, ${partition}")
    }
  }

  /**
    * Return transaction UUID
    */
  def getTransactionUUID(): UUID = transactionUuid

  /**
    * Return current transaction amount of data
    */
  def getDataItemsCount() = data.lastOffset

  /**
    * Returns Transaction owner
    */
  def getTransactionOwner() = txnOwner

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
    val number = data.put(obj, txnOwner.producerOptions.converter)
    val job = {
      if (number % txnOwner.producerOptions.batchSize == 0) {
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
    txnOwner.stream.metadataStorage.commitEntity.deleteAsync(
      streamName  = txnOwner.stream.getName,
      partition   = partition,
      transaction = transactionUuid,
      executor    = txnOwner.p2pAgent.getCassandraAsyncExecutor,
      function    = () => {
        val msg = Message(txnUuid = transactionUuid,
          ttl = -1,
          status = TransactionStatus.cancel,
          partition = partition)
        txnOwner.p2pAgent.publish(msg)
        if(Transaction.logger.isDebugEnabled)
        {
          Transaction.logger.debug(s"[CANCEL PARTITION_${msg.partition}] ts=${msg.txnUuid.toString} status=${msg.status.toString}")
        }
      })
  }

  /**
    * Canceling current transaction
    */
  def cancel() = {
    state.awaitMaterialization(txnOwner.producerOptions.coordinationOptions.transport.getTimeout())
    LockUtil.withLockOrDieDo[Unit](transactionLock, (100, TimeUnit.SECONDS), Some(Transaction.logger), () => {
      state.awaitUpdateComplete
      state.closeOrDie
      txnOwner.backendActivityService.submit(new Runnable {
        override def run(): Unit = cancelAsync()
      }, Option(transactionLock))
    })
  }

  private def checkpointPostEventPart() : Unit = {
    if(Transaction.logger.isDebugEnabled) {
      Transaction.logger.debug(s"[COMMIT PARTITION_{}] ts={}", partition, transactionUuid.toString)
    }
    //debug purposes only
    {
      val interruptExecution: Boolean = try {
        GlobalHooks.invoke(GlobalHooks.afterCommitFailure)
        false
      } catch {
        case e: Exception =>
          Transaction.logger.warn("AfterCommitFailure in DEBUG mode")
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

    if(Transaction.logger.isDebugEnabled) {
      Transaction.logger.debug("[FINAL CHECKPOINT PARTITION_{}] ts={}", partition, transactionUuid.toString)
    }
  }


  private def checkpointAsync() : Unit = {
    finalizeDataSend()
    //close transaction using stream ttl
    if (getDataItemsCount > 0) {
      jobs.foreach(x => x())

      if(Transaction.logger.isDebugEnabled) {
        Transaction.logger.debug("[START PRE CHECKPOINT PARTITION_{}] ts={}", partition, transactionUuid.toString)
      }

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
            Transaction.logger.warn("PreCommitFailure in DEBUG mode")
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
        totalCnt = getDataItemsCount,
        ttl = txnOwner.stream.getTTL,
        executor = txnOwner.p2pAgent.getCassandraAsyncExecutor,
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
    LockUtil.withLockOrDieDo[Unit](transactionLock, (100, TimeUnit.SECONDS), Some(Transaction.logger), () => {
      state.awaitUpdateComplete
      state.closeOrDie
      if (!isSynchronous) {
        txnOwner.backendActivityService.submit(new Runnable {
          override def run(): Unit = checkpointAsync()
        }, Option(transactionLock))
      }
      else {
        finalizeDataSend()

        if (getDataItemsCount > 0) {
          jobs.foreach(x => x())

          if(Transaction.logger.isDebugEnabled) {
            Transaction.logger.debug("[START PRE CHECKPOINT PARTITION_{}] ts={}", partition, transactionUuid.toString)
          }

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
            totalCnt = getDataItemsCount,
            ttl = txnOwner.stream.getTTL)

          if(Transaction.logger.isDebugEnabled) {
            Transaction.logger.debug("[COMMIT PARTITION_{}] ts={}", partition, transactionUuid.toString)
          }
          //debug purposes only
          GlobalHooks.invoke(GlobalHooks.afterCommitFailure)

          txnOwner.p2pAgent.publish(Message(
            txnUuid = transactionUuid,
            ttl = -1,
            status = TransactionStatus.postCheckpoint,
            partition = partition))

          if(Transaction.logger.isDebugEnabled) {
            Transaction.logger.debug("[FINAL CHECKPOINT PARTITION_{}] ts={}", partition, transactionUuid.toString)
          }
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

    if(Transaction.logger.isDebugEnabled) {
      Transaction.logger.debug(s"[KEEP_ALIVE THREAD PARTITION_PARTITION_${partition}] ts=${transactionUuid.toString} status=${TransactionStatus.update}")
    }
  }

  def updateTxnKeepAliveState(): Unit = {
    if(!state.isMaterialized)
      return
    // atomically check state and launch update process
    val stateOnUpdateClosed =
      LockUtil.withLockOrDieDo[Boolean](transactionLock, (100, TimeUnit.SECONDS), Some(Transaction.logger), () => {
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
    if(Transaction.logger.isDebugEnabled) {
      Transaction.logger.debug("Update event for txn {}, partition: {}", transactionUuid, partition)
    }

    val f = txnOwner.stream.metadataStorage.commitEntity.commitAsync(
    streamName = txnOwner.stream.getName,
    partition = partition,
    transaction = transactionUuid,
    totalCnt = -1,
    ttl = txnOwner.producerOptions.transactionTTL,
    executor = txnOwner.p2pAgent.getCassandraAsyncExecutor,
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
      txnUuid = getTransactionUUID,
      ttl = -1,
      status = TransactionStatus.preCheckpoint,
      partition = partition)

    val finalCheckpoint = Message(
      txnUuid = getTransactionUUID,
      ttl = -1,
      status = TransactionStatus.postCheckpoint,
      partition = partition)

    ProducerCheckpointInfo(transactionRef = this,
      agent = txnOwner.p2pAgent,
      preCheckpointEvent = preCheckpoint,
      finalCheckpointEvent = finalCheckpoint,
      streamName = txnOwner.stream.getName,
      partition = partition,
      transaction = getTransactionUUID,
      totalCnt = getDataItemsCount,
      ttl = txnOwner.stream.getTTL)
  }
}