package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.UUID

import com.bwsw.tstreams.coordination.messages.state.TransactionStatus.ProducerTransactionStatus

/**
  * Created by Ivan Kudryavtsev on 19.08.16.
  */
case class TransactionState(uuid: UUID,
                            partition: Int,
                            var masterSessionID: Int,
                            var queueOrderID: Long,
                            var itemCount: Int,
                            var state: ProducerTransactionStatus,
                            var ttl: Long) {
  override def toString() = {
    s"TransactionState($uuid, $partition, $masterSessionID, $queueOrderID, $itemCount, $state, $ttl)"
  }
}
