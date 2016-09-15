package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor

import scala.collection.mutable

/**
  * Created by Ivan Kudryavtsev at 20.08.2016
  */
class TransactionBufferWorker() {
  private val updateExecutor = new FirstFailLockableTaskExecutor("TransactionBufferWorker-updateExecutor")
  val transactionBufferMap = mutable.Map[Int, TransactionBuffer]()
  val isComplete = new AtomicBoolean(false)

  val signalThread = new Thread(new Runnable {
    override def run(): Unit = {
      while(!isComplete.get)
        signalTransactionStateSequences()
    }
  })

  signalThread.start()

  def assign(partition: Int, transactionBuffer: TransactionBuffer) = this.synchronized {
    if (!transactionBufferMap.contains(partition))
      transactionBufferMap(partition) = transactionBuffer
    else
      throw new IllegalStateException(s"Partition $partition is bound already.")
  }

  def signalTransactionStateSequences() = this.synchronized {
    transactionBufferMap.foreach(kv => kv._2.signalCompleteTransactions())
  }

  def getPartitions() = transactionBufferMap.keySet

  /**
    * submits state to executor for offloaded computation
    *
    * @param transactionState
    */
  def update(transactionState: TransactionState) = {
    updateExecutor.submit(s"<UpdateAndNotifyTask($transactionState)>", new Runnable {
      override def run(): Unit = {
        transactionBufferMap(transactionState.partition).update(transactionState)
      }
    })
  }

  /**
    * stops executor
    */
  def stop() = {
    isComplete.set(true)
    signalThread.join()
    updateExecutor.shutdownOrDie(100, TimeUnit.SECONDS)
    transactionBufferMap.foreach(kv => kv._2.counters.dump(kv._1))
    transactionBufferMap.clear()
  }
}
