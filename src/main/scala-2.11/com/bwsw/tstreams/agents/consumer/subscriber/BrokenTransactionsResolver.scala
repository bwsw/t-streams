package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.coordination.pubsub.messages.ProducerTransactionStatus
import com.bwsw.tstreams.coordination.pubsub.messages.ProducerTransactionStatus._
import org.slf4j.LoggerFactory

import scala.collection.mutable

class BrokenTransactionsResolver(subscriber: SubscribingConsumer[_]) {
  private val MAX_RETRIES = 2
  private val UPDATE_INTERVAL = 5000
  private var updateThread: Thread = null
  private val isRunning = new AtomicBoolean(false)
  private val checkpointEventResolverLock = new ReentrantLock(true)
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val partitionToBuffer = mutable.Map[Int, TransactionBufferUtils]()
  private val partitionToTxns = mutable.Map[Int, mutable.Set[UUID]]()
  private val retries = mutable.Map[Int, mutable.Map[UUID, Int]]()

  def bindBuffer(partition: Int, buffer: TransactionsBuffer, lock: ReentrantLock, lastTxn: LastTransactionWrapper) = {
    checkpointEventResolverLock.lock()
    logger.debug(s"[CHECKPOINT EVENT RESOLVER] start bind buffer on partition:{$partition}")
    partitionToBuffer(partition) = TransactionBufferUtils(buffer, lock, lastTxn)
    logger.debug(s"[CHECKPOINT EVENT RESOLVER] finish bind buffer on partition:{$partition}")
    checkpointEventResolverLock.unlock()
  }

  def update(partition: Int, txn: UUID, status: ProducerTransactionStatus) = {
    checkpointEventResolverLock.lock()
    logger.debug(s"[CHECKPOINT EVENT RESOLVER] start update CER on partition:{$partition}" +
      s" with txn:{${txn.timestamp()}}")
    status match {
      case ProducerTransactionStatus.preCheckpoint =>
        if (partitionToTxns.contains(partition)) {
          assert(retries.contains(partition))
          retries(partition)(txn) = MAX_RETRIES
          partitionToTxns(partition) += txn
        } else {
          assert(!retries.contains(partition))
          retries(partition) = mutable.Map(txn -> MAX_RETRIES)
          partitionToTxns(partition) = mutable.Set[UUID](txn)
        }
        logger.debug(s"[CHECKPOINT EVENT RESOLVER] [UPDATE PRECHECKPOINT] CER on " +
          s"partition:{$partition}" +
          s" with txn:{${txn.timestamp()}}")

      case ProducerTransactionStatus.`postCheckpoint` =>
        removeTxn(partition, txn)
        logger.debug(s"[CHECKPOINT EVENT RESOLVER] [UPDATE FINALCHECKPOINT] CER on " +
          s"partition:{$partition}" +
          s" with txn:{${txn.timestamp()}}")
    }
    checkpointEventResolverLock.unlock()
  }

  def startUpdate() = {
    isRunning.set(true)
    updateThread = new Thread(new Runnable {
      override def run(): Unit = {
        while (isRunning.get()) {
          checkpointEventResolverLock.lock()
          refresh()
          checkpointEventResolverLock.unlock()
          Thread.sleep(UPDATE_INTERVAL)
        }
      }
    })
    updateThread.start()
  }

  def stop() = {
    isRunning.set(false)
    updateThread.join()
    partitionToBuffer.clear()
    partitionToTxns.clear()
    retries.clear()
  }

  private def removeTxn(partition: Int, txn: UUID) = {
    if (retries.contains(partition) && retries(partition).contains(txn)) {
      retries(partition).remove(txn)
      partitionToTxns(partition).remove(txn)
    }
  }

  private def updateTransactionBuffer(partition: Int,
                                      txn: UUID,
                                      status: ProducerTransactionStatus,
                                      ttl: Int) = {
    val transactionBufferUtils = partitionToBuffer(partition)
    transactionBufferUtils.lock.lock()
    if (txn.timestamp() > transactionBufferUtils.lastConsumedTxn.get().timestamp())
      transactionBufferUtils.buffer.update(txn, status, ttl)
    transactionBufferUtils.lock.unlock()
  }

  private def refresh() = {
    partitionToTxns foreach { case (partition, transactions) =>
      transactions foreach { txn =>
        if (retries(partition)(txn) == 0) {
          updateTransactionBuffer(partition, txn, ProducerTransactionStatus.cancel, -1)
          removeTxn(partition, txn)
          logger.debug(s"[CHECKPOINT EVENT RESOLVER] [REFRESH ZERO RETRY] CER on" +
            s" partition:{$partition}" +
            s" with txn:{${txn.timestamp()}}")
        } else {
          val updatedTransaction = subscriber.updateTransactionInfoFromDB(txn, partition)
          updatedTransaction match {
            case Some(transactionSettings) =>
              if (transactionSettings.totalItems != -1) {
                updateTransactionBuffer(partition, txn, ProducerTransactionStatus.postCheckpoint, -1)
                removeTxn(partition, txn)
                logger.debug(s"[CHECKPOINT EVENT RESOLVER] [REFRESH TB UPDATE] CER on" +
                  s" partition:{$partition}" +
                  s" with txn:{${txn.timestamp()}}")
              } else {
                retries(partition)(txn) -= 1
                logger.debug(s"[CHECKPOINT EVENT RESOLVER] [REFRESH RETRY DECREASE] CER on" +
                  s" partition:{$partition}" +
                  s" with txn:{${txn.timestamp()}}")
              }

            case None =>
              //the transaction has been deleted by cassandra so we need to remove it from transaction buffer
              updateTransactionBuffer(partition, txn, ProducerTransactionStatus.cancel, -1)
              removeTxn(partition, txn)
          }
        }
      }
    }
  }
}

case class TransactionBufferUtils(buffer: TransactionsBuffer, lock: ReentrantLock, lastConsumedTxn: LastTransactionWrapper)