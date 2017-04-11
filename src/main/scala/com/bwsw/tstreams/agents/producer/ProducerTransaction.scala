package com.bwsw.tstreams.agents.producer

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.group.ProducerCheckpointInfo
import com.bwsw.tstreams.common.{LockUtil, ResettableCountDownLatch}
import com.bwsw.tstreams.coordination.messages.state.{TransactionStateMessage, TransactionStatus}
import com.bwsw.tstreams.debug.GlobalHooks
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
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
  * @param transactionID    ID for this transaction
  */
class ProducerTransaction(partition: Int,
                          transactionID: Long,
                          transactionOwner: Producer) extends IProducerTransaction {

  private val transactionLock = new ReentrantLock()

  private val data = new ProducerTransactionData(this, transactionOwner.stream.ttl, transactionOwner.stream.client)

  private val isTransactionClosed = new AtomicBoolean(false)

  /**
    * This special trigger is used to avoid a very specific race, which could happen if
    * checkpoint/cancel will be called same time when update will do update. So, we actually
    * must protect from this situation, that's why during update, latch must be set to make
    * cancel/checkpoint wait until update will complete
    * We also need special test for it.
    */
  private val updateSignalVar = new ResettableCountDownLatch(0)

  /**
    * State indicator of the transaction
    *
    * @return Closed transaction or not
    */
  def isClosed() = isTransactionClosed.get

  /**
    * changes transaction state to updating
    */
  def setUpdateInProgress() = updateSignalVar.setValue(1)

  /**
    * changes transaction state to finished updating
    */
  def setUpdateFinished() = updateSignalVar.countDown

  /**
    * awaits while transaction updates
    *
    * @return
    */
  def awaitUpdateComplete(): Unit = {
    if (!updateSignalVar.await(10, TimeUnit.SECONDS))
      throw new IllegalStateException("Update takes too long (> 10 seconds). Probably failure.")
  }


  /**
    * BasicProducerTransaction logger for logging
    */
  ProducerTransaction.logger.debug(s"Open transaction $getTransactionID for\nstream, partition: ${transactionOwner.stream.name}, ${}")

  /**
    *
    */
  def markAsClosed() = isTransactionClosed.set(true)

  /**
    * Return transaction partition
    */
  def getPartition(): Int = partition


  override def toString(): String = s"producer.Transaction(ID=$transactionID, partition=$partition, count=${getDataItemsCount()})"

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
  def send(obj: Array[Byte]): Unit = {
    if (isTransactionClosed.get())
      throw new IllegalStateException(s"Transaction $transactionID is closed. New data items are prohibited.")

    val number = data.put(obj)
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

  def send(string: String): Unit = send(string.getBytes())

  /**
    * Does actual send of the data that is not sent yet
    */
  def finalizeDataSend(): Unit = {
    val job = data.save()
    if (job != null) jobs += job
  }


  /**
    * Canceling current transaction
    */
  def cancel(): Unit = this.synchronized {
    if (isTransactionClosed.get())
      throw new IllegalStateException(s"Transaction $transactionID is closed. New data items are prohibited.")

    awaitUpdateComplete
    isTransactionClosed.set(true)
    val transactionRecord = new RPCProducerTransaction(transactionOwner.stream.name, partition, transactionID, TransactionStates.Cancel, 0, -1L)

    transactionOwner.stream.client.putTransaction(transactionRecord, true)(rec => {})

    val msg = TransactionStateMessage(transactionID = transactionID,
      ttlMs = -1,
      status = TransactionStatus.cancel,
      partition = partition,
      masterID = transactionOwner.getPartitionMasterIDLocalInfo(partition),
      orderID = -1,
      count = 0)
    transactionOwner.p2pAgent.publish(msg)
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
      ttlMs = -1,
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
      val transactionRecord = new RPCProducerTransaction(transactionOwner.stream.name, partition, transactionID, TransactionStates.Checkpointed, getDataItemsCount(), transactionOwner.stream.ttl)

      transactionOwner.stream.client.putTransaction(transactionRecord, true)(record => {
        checkpointPostEventPart()
      })


    }
    else {
      transactionOwner.p2pAgent.publish(TransactionStateMessage(
        transactionID = transactionID,
        ttlMs = -1,
        status = TransactionStatus.cancel,
        partition = partition,
        masterID = transactionOwner.getPartitionMasterIDLocalInfo(partition),
        orderID = -1, count = 0))
    }
  }

  /**
    * Submit transaction(transaction will be available by consumer only after closing)
    */
  def checkpoint(isSynchronous: Boolean = true): Unit = {
    if (isTransactionClosed.get())
      throw new IllegalStateException(s"Transaction $transactionID is closed. New data items are prohibited.")
    LockUtil.withLockOrDieDo[Unit](transactionLock, (100, TimeUnit.SECONDS), Some(ProducerTransaction.logger), () => {
      awaitUpdateComplete()
      isTransactionClosed.set(true)

      if (!isSynchronous) {
        transactionOwner.asyncActivityService.submit("<CheckpointAsyncTask>", () => checkpointAsync(), Option(transactionLock))
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
          val transactionRecord = new RPCProducerTransaction(transactionOwner.stream.name, partition, transactionID, TransactionStates.Checkpointed, getDataItemsCount(), transactionOwner.stream.ttl)

          transactionOwner.stream.client.putTransaction(transactionRecord, true)(record => {
            latch.countDown()
          })
          latch.await()

          if (ProducerTransaction.logger.isDebugEnabled) {
            ProducerTransaction.logger.debug("[COMMIT PARTITION_{}] ts={}", partition, transactionID.toString)
          }
          //debug purposes only
          GlobalHooks.invoke(GlobalHooks.afterCommitFailure)

          transactionOwner.p2pAgent.submitPipelinedTaskToPublishExecutors(partition, () =>
            transactionOwner.p2pAgent.publish(TransactionStateMessage(
              transactionID = transactionID,
              ttlMs = -1,
              status = TransactionStatus.checkpointed,
              partition = partition,
              masterID = transactionOwner.getPartitionMasterIDLocalInfo(partition),
              orderID = -1,
              count = getDataItemsCount())))

          if (ProducerTransaction.logger.isDebugEnabled) {
            ProducerTransaction.logger.debug("[FINAL CHECKPOINT PARTITION_{}] ts={}", partition, transactionID.toString)
          }
        }
        else {
          transactionOwner.p2pAgent.submitPipelinedTaskToPublishExecutors(partition, () =>
            transactionOwner.p2pAgent.publish(TransactionStateMessage(
              transactionID = transactionID,
              ttlMs = -1,
              status = TransactionStatus.cancel,
              partition = partition,
              masterID = transactionOwner.getPartitionMasterIDLocalInfo(partition),
              orderID = -1,
              count = 0)))
        }
      }
    })
  }

  private def doSendUpdateMessage() = {
    {
      GlobalHooks.invoke(GlobalHooks.transactionUpdateTaskEnd)
    }

    setUpdateFinished

    transactionOwner.p2pAgent.publish(TransactionStateMessage(
      transactionID = transactionID,
      ttlMs = transactionOwner.producerOptions.transactionTtlMs,
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
    // atomically check state and launch update process
    val stateOnUpdateClosed =
      LockUtil.withLockOrDieDo[Boolean](transactionLock, (100, TimeUnit.SECONDS), Some(ProducerTransaction.logger), () => {
        val s = isClosed()
        if (!s) setUpdateInProgress()
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
    val transactionRecord = new RPCProducerTransaction(transactionOwner.stream.name, partition, transactionID, TransactionStates.Updated, -1, transactionOwner.producerOptions.transactionTtlMs)

    transactionOwner.stream.client.putTransaction(transactionRecord, true)(record => {
      doSendUpdateMessage()
    })

  }

  /**
    * accessor to lock object for external agents
    *
    * @return
    */
  def getTransactionLock(): ReentrantLock = transactionLock


  def getTransactionInfo(): ProducerCheckpointInfo = {

    val checkpoint = TransactionStateMessage(
      transactionID = getTransactionID(),
      ttlMs = -1,
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