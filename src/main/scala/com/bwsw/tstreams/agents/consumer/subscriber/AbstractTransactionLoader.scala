package com.bwsw.tstreams.agents.consumer.subscriber

import com.bwsw.tstreams.agents.consumer.TransactionOperator
import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor

/**
  * Created by Ivan Kudryavtsev on 22.08.16.
  */
trait AbstractTransactionLoader {
  def checkIfTransactionLoadingIsPossible(seq: QueueBuilder.QueueItemType): Boolean

  def load(seq: QueueBuilder.QueueItemType,
              consumer: TransactionOperator,
              executor: FirstFailLockableTaskExecutor,
              callback: Callback)
}
