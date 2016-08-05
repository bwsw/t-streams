package com.bwsw.tstreams.agents.group

import java.util.UUID

import com.bwsw.tstreams.agents.producer.Transaction
import com.bwsw.tstreams.coordination.pubsub.messages.ProducerTopicMessage
import com.bwsw.tstreams.coordination.transactions.peertopeer.PeerToPeerAgent

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
  *                       first - do precheckpoint event for all subscribers
  *                       second - commit txn metadata in cassandra
  *                       third - do finalcheckpoint event for all subscribers
  * @param preCheckpointEvent
  * @param finalCheckpointEvent
  * @param streamName     Stream name
  * @param partition      Partition number
  * @param transaction    Transaction to commit
  * @param totalCnt       Total info in transaction
  * @param ttl            Transaction time to live in seconds
  */
case class ProducerCheckpointInfo(transactionRef: Transaction[_],
                                  agent: PeerToPeerAgent,
                                  preCheckpointEvent: ProducerTopicMessage,
                                  finalCheckpointEvent: ProducerTopicMessage,
                                  streamName: String,
                                  partition: Int,
                                  transaction: UUID,
                                  totalCnt: Int,
                                  ttl: Int) extends CheckpointInfo

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
                                  offset: UUID) extends CheckpointInfo
