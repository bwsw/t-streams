package com.bwsw.tstreams.agents.consumer.subscriber_v2

import com.bwsw.tstreams.agents.consumer.Consumer
import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor

/**
  * Created by ivan on 22.08.16.
  */
trait AbstractTransactionLoader {
  def checkIfPossible(seq: QueueBuilder.QueueItemType): Boolean
  def load[T](seq: QueueBuilder.QueueItemType,
              consumer: Consumer[T],
              executor: FirstFailLockableTaskExecutor,
              callback: Callback[T])
}
