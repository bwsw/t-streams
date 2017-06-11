package com.bwsw.tstreams.agents.group

import com.bwsw.tstreams.agents.producer.{Producer, ProducerTransactionImpl}
import com.bwsw.tstreamstransactionserver.protocol.TransactionState

/**
  * Basic commit trait
  */
sealed trait State

/**
  * BasicProducer commit information
  *
  * @param transactionRef Reference on transaction (used for obliterate update thread)
  * @param agent          Producer agent for sending events
  *                       every transaction followed with three actions
  *                       first - do pre checkpoint event for all subscribers
  *                       second - commit transaction metadata in database
  *                       third - do final checkpoint event for all subscribers
  * @param event
  * @param streamID     Stream name
  * @param partition      Partition number
  * @param transaction    Transaction to commit
  * @param totalCnt       Total info in transaction
  * @param ttl            Transaction time to live in seconds
  */
case class ProducerTransactionState(transactionRef: ProducerTransactionImpl,
                                    agent: Producer,
                                    event: TransactionState,
                                    streamID: Int,
                                    partition: Int,
                                    transaction: Long,
                                    totalCnt: Int,
                                    ttl: Long) extends State

/**
  * BasicConsumer commit information
  *
  * @param name      Concrete consumer name
  * @param streamID    Stream name
  * @param partition Partition number
  * @param offset    Offset to commit
  */
case class ConsumerState(name: String,
                         streamID: Int,
                         partition: Int,
                         offset: Long) extends State
