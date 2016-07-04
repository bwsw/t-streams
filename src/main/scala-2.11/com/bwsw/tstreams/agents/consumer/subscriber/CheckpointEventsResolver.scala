package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.coordination.pubsub.messages.ProducerTransactionStatus
import com.bwsw.tstreams.coordination.pubsub.messages.ProducerTransactionStatus.ProducerTransactionStatus
import org.slf4j.LoggerFactory

import scala.collection._

class CheckpointEventsResolver(subscriber : BasicSubscribingConsumer[_,_]) {
  private val UPDATE_INTERVAL = 5000
  private val MAX_RETRIES = 2

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val partitionToBuffer = mutable.Map[Int, (TransactionsBuffer, ReentrantLock)]()
  private val partitionToTxns = mutable.Map[Int, mutable.Set[UUID]]()
  private val retries = mutable.Map[Int, mutable.Map[UUID, Int]]()
  private val checkpointEventResolverLock = new ReentrantLock(true)
  private val isRunning = new AtomicBoolean(false)
  private var updateThread : Thread = null

  def bindBuffer(partition : Int, buffer : TransactionsBuffer, lock : ReentrantLock) = {
    checkpointEventResolverLock.lock()
    logger.debug(s"[CHECKPOINT EVENT RESOLVER] start bind buffer on partition:{$partition}")
    partitionToBuffer(partition) = buffer -> lock
    logger.debug(s"[CHECKPOINT EVENT RESOLVER] finish bind buffer on partition:{$partition}")
    checkpointEventResolverLock.unlock()
  }

  def update(partition : Int, txn : UUID, status : ProducerTransactionStatus) = {
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

      case ProducerTransactionStatus.finalCheckpoint =>
        removeTxn(partition, txn)
        logger.debug(s"[CHECKPOINT EVENT RESOLVER] [UPDATE FINALCHECKPOINT] CER on " +
          s"partition:{$partition}" +
          s" with txn:{${txn.timestamp()}}")
    }
    checkpointEventResolverLock.unlock()
  }

  private def removeTxn(partition : Int, txn : UUID) = {
    retries(partition).remove(txn)
    partitionToTxns(partition).remove(txn)
  }

  private def updateTransactionBuffer(partition : Int,
                                      txn : UUID,
                                      status : ProducerTransactionStatus,
                                      ttl : Int) = {
    val (buffer,lock) = partitionToBuffer(partition)
    lock.lock()
    buffer.update(txn, status, ttl)
    lock.unlock()
  }

  private def refresh() = {
    partitionToTxns foreach { case (partition, transactions) =>
      transactions foreach { txn =>
        if (retries(partition)(txn) == 0) {
          removeTxn(partition, txn)
          logger.debug(s"[CHECKPOINT EVENT RESOLVER] [REFRESH ZERO RETRY] CER on" +
            s" partition:{$partition}" +
            s" with txn:{${txn.timestamp()}}")
        } else {
          val updatedTransaction = subscriber.updateTransaction(txn, partition)
          updatedTransaction match {
            case Some(transactionSettings) =>
              if (transactionSettings.totalItems != -1){
                updateTransactionBuffer(partition, txn, ProducerTransactionStatus.finalCheckpoint, -1)
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
              updateTransactionBuffer(partition, txn, ProducerTransactionStatus.cancelled, -1)
              removeTxn(partition,txn)
          }
        }
      }
    }
  }

  def startUpdate() = {
    isRunning.set(true)
    clear()
    updateThread = new Thread(new Runnable {
      override def run(): Unit = {
        while(isRunning.get()){
          checkpointEventResolverLock.lock()
          refresh()
          checkpointEventResolverLock.unlock()
          Thread.sleep(UPDATE_INTERVAL)
        }
      }
    })
    updateThread.start()
    logger.debug(s"[CHECKPOINT EVENT RESOLVER] started")
  }

  def stop() = {
    isRunning.set(false)
    updateThread.join()
    logger.debug(s"[CHECKPOINT EVENT RESOLVER] stoped")
  }

  private def clear() = {
    partitionToBuffer.clear()
    partitionToTxns.clear()
    retries.clear()
  }
}
