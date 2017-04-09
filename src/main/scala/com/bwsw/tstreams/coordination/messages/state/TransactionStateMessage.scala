package com.bwsw.tstreams.coordination.messages.state

import com.bwsw.tstreams.coordination.messages.state.TransactionStatus.ProducerTransactionStatus

/**
  * Class which describes transaction states. This class is used to give to know subscribers about transactions.
  *
  * @param transactionID
  * @param ttlMs
  * @param status
  * @param partition
  * @param masterID
  * @param orderID
  */
case class TransactionStateMessage(transactionID: Long,
                                   ttlMs: Long,
                                   status: ProducerTransactionStatus,
                                   partition: Int,
                                   masterID: Int,
                                   orderID: Long,
                                   count: Int)

