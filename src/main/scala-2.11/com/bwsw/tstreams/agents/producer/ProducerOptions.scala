package com.bwsw.tstreams.agents.producer

import java.net.InetSocketAddress

import com.bwsw.tstreams.common.AbstractPolicy
import com.bwsw.tstreams.converter.IConverter
import com.bwsw.tstreams.coordination.client.TcpTransport
import com.bwsw.tstreams.generator.ITransactionGenerator

import scala.language.existentials

/**
  * @param transactionTTL               Single transaction live time
  * @param transactionKeepAliveInterval Update transaction interval which is used to keep alive transaction in time when it is opened
  * @param writePolicy                  Strategy for selecting next partition
  * @param converter                    User defined or basic converter for converting T objects to Array[Byte] objects(storage object type)
  * @param batchSize                    Insertion Type (only BatchInsert and SingleElementInsert are allowed now)
  * @param transactionGenerator         Generator for generating IDs
  * @tparam T User object type
  */
class ProducerOptions[T](val transactionTTL: Int,
                         val transactionKeepAliveInterval: Int,
                         val writePolicy: AbstractPolicy,
                         val batchSize: Int,
                         val transactionGenerator: ITransactionGenerator,
                         val coordinationOptions: CoordinationOptions,
                         val converter: IConverter[T, Array[Byte]]) {

  /**
    * Transaction minimum ttl time
    */
  private val minTransactionTTL = 3

  /**
    * Options validating
    */
  if (transactionTTL < minTransactionTTL)
    throw new IllegalArgumentException(s"Option transactionTTL must be greater or equal than $minTransactionTTL")

  if (transactionKeepAliveInterval < 1)
    throw new IllegalArgumentException(s"Option transactionKeepAliveInterval must be greater or equal than 1")

  if (transactionKeepAliveInterval.toDouble > transactionTTL.toDouble / 3.0)
    throw new IllegalArgumentException("Option transactionTTL must be at least three times greater or equal than transaction")

  if (batchSize <= 0)
    throw new IllegalArgumentException("Batch size must be greater or equal 1")

}


/**
  * @param zkHosts                 Zk hosts to connect
  * @param zkRootPath              Zk root path for all metadata
  * @param zkSessionTimeout        Zk session timeout
  * @param isLowPriorityToBeMaster Flag which indicate priority to became master on stream/partition
  *                                of this agent
  * @param transport               Transport providing interaction between agents
  * @param threadPoolAmount        Thread pool amount which is used by
  *                                PeerAgent
  *                                by default (threads_amount == used_producer_partitions)
  */
class CoordinationOptions(val zkHosts: List[InetSocketAddress],
                          val zkRootPath: String,
                          val zkSessionTimeout: Int,
                          val zkConnectionTimeout: Int,
                          val isLowPriorityToBeMaster: Boolean,
                          val transport: TcpTransport,
                          val threadPoolAmount: Int,
                          val threadPoolPublisherThreadsAmount: Int,
                          val partitionRedistributionDelay: Int,
                          val isMasterBootstrapModeFull: Boolean,
                          val isMasterProcessVote: Boolean)