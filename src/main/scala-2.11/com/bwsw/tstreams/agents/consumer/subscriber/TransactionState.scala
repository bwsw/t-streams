package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.UUID

import com.bwsw.tstreams.coordination.messages.state.TransactionStatus.ProducerTransactionStatus

/**
  * Created by Ivan Kudryavtsev on 19.08.16.
  */
case class TransactionState(uuid: UUID,
                            partition: Int,
                            masterSessionID: Int,
                            queueOrderID: Long,
                            itemCount: Int,
                            state: ProducerTransactionStatus,
                            ttl: Int) {
  override def toString() = {
    s"TransactionState($uuid, $partition, $masterSessionID, $queueOrderID, $itemCount, $state, $ttl)"
  }
}
