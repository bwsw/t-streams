package com.bwsw.tstreams.agents.consumer.subscriber_v2

import java.util.UUID
import com.bwsw.tstreams.common.{UUIDComparator, SortedExpiringMap}

/**
  * Created by ivan on 19.08.16.
  */
class TransactionBuffer(partition: Int,
                        queue: QueueBuilder.QueueType) {

  private val map: SortedExpiringMap[UUID, TransactionState] =
    new SortedExpiringMap(new UUIDComparator, new TransactionStateExpirationPolicy)

  def signal(state: TransactionState) = {
  }

  def signalCompleteTransactions() = {

  }
}
