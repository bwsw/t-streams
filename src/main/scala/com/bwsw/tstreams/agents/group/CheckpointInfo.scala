package com.bwsw.tstreams.agents.group

import com.bwsw.tstreams.agents.producer.{ProducerTransaction, TransactionOpenerService}
import com.bwsw.tstreams.proto.protocol.TransactionState

import scala.language.existentials


/**
  * Basic commit trait
  */
sealed trait CheckpointInfo

/**
  * BasicProducer commit information
  *
  * @param transactionRef Reference on transaction (used for obliterate update thread)
  * @param agent          Producer agent for sending events
  *                       every transaction followed with three actions
  *                       first - do pre checkpoint event for all subscribers
  *                       second - commit transaction metadata in database
  *                       third - do final checkpoint event for all subscribers
  * @param checkpointEvent
  * @param streamName     Stream name
  * @param partition      Partition number
  * @param transaction    Transaction to commit
  * @param totalCnt       Total info in transaction
  * @param ttl            Transaction time to live in seconds
  */
case class ProducerCheckpointInfo(transactionRef: ProducerTransaction,
                                  agent: TransactionOpenerService,
                                  checkpointEvent: TransactionState,
                                  streamName: String,
                                  partition: Int,
                                  transaction: Long,
                                  totalCnt: Int,
                                  ttl: Long) extends CheckpointInfo

/**
  * BasicConsumer commit information
  *
  * @param name      Concrete consumer name
  * @param stream    Stream name
  * @param partition Partition number
  * @param offset    Offset to commit
  */
case class ConsumerCheckpointInfo(name: String,
                                  stream: String,
                                  partition: Int,
                                  offset: Long) extends CheckpointInfo
