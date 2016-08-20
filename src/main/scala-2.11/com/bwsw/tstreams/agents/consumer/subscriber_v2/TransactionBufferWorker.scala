package com.bwsw.tstreams.agents.consumer.subscriber_v2

import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor

/**
  * Created by Ivan Kudryavtsev at 20.08.2016
  */
class TransactionBufferWorker(transactionBuffer: TransactionBuffer) {
  private val executor = new FirstFailLockableTaskExecutor("TransactionBufferWorker-Executor")

  /**
    *
    * @param transactionState
    */
  def updateAndNotify(transactionState: TransactionState) = {
    executor.submit(new Runnable {
      override def run(): Unit = {
        transactionBuffer.update(transactionState)
        transactionBuffer.signalCompleteTransactions()
      }
    })
  }

}
