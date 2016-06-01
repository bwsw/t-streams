package com.bwsw.tstreams.agents.group

import java.util.UUID


/**
 * Basic commit trait
 */
trait CommitInfo

/**
 * BasicProducer commit information (not used now)
 * @param streamName Stream name
 * @param partition Partition number
 * @param transaction Transaction to commit
 * @param totalCnt Total info in transaction
 * @param ttl Transaction time to live in seconds
 */
case class ProducerCommitInfo(streamName : String, partition : Int, transaction: UUID, totalCnt : Int, ttl : Int) extends CommitInfo

/**
 * BasicConsumer commit information
 * @param name Concrete consumer name
 * @param stream Stream name
 * @param partition Partition number
 * @param offset Offset to commit
 */
case class ConsumerCommitInfo(name : String, stream : String, partition : Int, offset : UUID) extends CommitInfo
