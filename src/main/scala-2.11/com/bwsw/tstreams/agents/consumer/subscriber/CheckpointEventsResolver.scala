package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.coordination.pubsub.messages.ProducerTransactionStatus
import com.bwsw.tstreams.coordination.pubsub.messages.ProducerTransactionStatus.ProducerTransactionStatus

import scala.collection._

class CheckpointEventsResolver(subscriber : BasicSubscribingConsumer[_,_]) {
  //TODO must be updated first on incoming event
  private val MAX_RETRIES = 5
  private val partitionToBuffer = mutable.Map[Int, TransactionsBuffer]()
  private val partitionToTxns = mutable.Map[Int, mutable.Set[UUID]]()
  private val retries = mutable.Map[Int, mutable.Map[UUID, Int]]()
  private val lock = new ReentrantLock(true)
  private val isRunning = new AtomicBoolean(false)
  private var updateThread : Thread = null

  def bindBuffer(partition : Int, buffer : TransactionsBuffer) = {
    partitionToBuffer(partition) = buffer
  }

  def updateTransaction(partition : Int, txn : UUID, status : ProducerTransactionStatus) = {
    status match {
      case ProducerTransactionStatus.preCheckpoint =>
        lock.lock()
        if (partitionToTxns.contains(partition)) {
          partitionToTxns(partition) += txn
        } else {
          partitionToTxns(partition) = mutable.Set[UUID]()
        }
        retries(partition)(txn) = MAX_RETRIES
        lock.unlock()

      case ProducerTransactionStatus.finalCheckpoint =>
        lock.lock()
        removeTxn(partition, txn)
        lock.unlock()

      case _ =>
        throw new IllegalStateException("Checkpoint event resolver incorrect state")
    }
  }

  private def removeTxn(partition : Int, txn : UUID) = {
    retries(partition).remove(txn)
    partitionToTxns(partition).remove(txn)
  }

  private def refresh() = {
    partitionToTxns foreach { case (partition, transactions) =>
      transactions foreach { txn =>
        if (retries(partition)(txn) == 0) {
          removeTxn(partition, txn)
        } else {
          val updatedTransaction = subscriber.updateTransaction(txn, partition)
          updatedTransaction match {
            case Some(transactionSettings) =>
              if (transactionSettings.totalItems != -1){
                partitionToBuffer(partition).update(txn, ProducerTransactionStatus.finalCheckpoint, -1)
                removeTxn(partition, txn)
              } else {
                retries(partition)(txn) -= 1
              }

            case None =>
              removeTxn(partition,txn)
          }
        }
      }
    }
  }

  def start(updateInterval : Int) = {
    isRunning.set(true)
    updateThread = new Thread(new Runnable {
      override def run(): Unit = {
        while(isRunning.get()){
          lock.lock()
          refresh()
          lock.unlock()
          Thread.sleep(updateInterval)
        }
      }
    })
    updateThread.start()
  }

  def stop() = {
    isRunning.set(false)
    updateThread.join()
    clear()
  }


  private def clear() = {
    partitionToBuffer.clear()
    partitionToTxns.clear()
    retries.clear()
  }
}
