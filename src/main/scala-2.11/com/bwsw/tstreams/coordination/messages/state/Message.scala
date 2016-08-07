package com.bwsw.tstreams.coordination.messages.state

import java.util.UUID

import com.bwsw.tstreams.coordination.messages.state.TransactionStatus.ProducerTransactionStatus

/**
  * Messages which is published by producer on every transaction update
  *
  * @param txnUuid   Transaction uuid
  * @param ttl       Time of transaction expiration in seconds
  * @param status    Transaction status
  * @param partition Transaction partition
  */
case class Message(txnUuid: UUID, ttl: Int, status: ProducerTransactionStatus, partition: Int)

