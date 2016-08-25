package com.bwsw.tstreams.coordination.messages.state

import java.util.UUID

import com.bwsw.tstreams.coordination.messages.state.TransactionStatus.ProducerTransactionStatus

/**
  * Class which describes transaction states. This class is used to give to know subscribers about transactions.
  * @param txnUuid
  * @param ttl
  * @param status
  * @param partition
  * @param masterID
  * @param orderID
  */
case class TransactionStateMessage( txnUuid: UUID,
                                    ttl: Int,
                                    status: ProducerTransactionStatus,
                                    partition: Int,
                                    masterID: Int,
                                    orderID: Long)

