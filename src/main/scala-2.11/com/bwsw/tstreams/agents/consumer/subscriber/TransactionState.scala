package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.UUID

import com.bwsw.tstreams.coordination.messages.state.TransactionStatus.ProducerTransactionStatus

/**
  * Created by ivan on 19.08.16.
  */
case class TransactionState(uuid: UUID,
                            partition: Int,
                            masterSessionID: Int,
                            queueOrderID: Long,
                            itemCount: Int,
                            state: ProducerTransactionStatus,
                            ttl: Int)
