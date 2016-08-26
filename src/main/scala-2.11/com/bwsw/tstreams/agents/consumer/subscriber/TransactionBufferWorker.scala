package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.concurrent.TimeUnit

import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor

import scala.collection.mutable

/**
  * Created by Ivan Kudryavtsev at 20.08.2016
  */
class TransactionBufferWorker() {
  private val executor = new FirstFailLockableTaskExecutor("TransactionBufferWorker-Executor")
  val transactionBufferMap = mutable.Map[Int, TransactionBuffer]()

  def assign(partition: Int, transactionBuffer: TransactionBuffer) = {
    if(!transactionBufferMap.contains(partition))
      transactionBufferMap(partition) = transactionBuffer
    else
      throw new IllegalStateException(s"Partition ${partition} is bound already.")
  }

  def getPartitions() = transactionBufferMap.keySet

  /**
    * submits state to executor for offloaded computation
    * @param transactionState
    */
  def updateAndNotify(transactionState: TransactionState) = {
    executor.submit(new Runnable {
      override def run(): Unit = {
        transactionBufferMap(transactionState.partition).update(transactionState)
        transactionBufferMap(transactionState.partition).signalCompleteTransactions()
      }
    })
  }

  /**
    * stops executor
    */
  def stop() = {
    executor.shutdownOrDie(100, TimeUnit.SECONDS)
    transactionBufferMap.clear()
  }
}
