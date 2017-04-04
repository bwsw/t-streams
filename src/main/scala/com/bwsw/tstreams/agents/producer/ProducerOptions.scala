package com.bwsw.tstreams.agents.producer

import com.bwsw.tstreams.common.AbstractPolicy
import com.bwsw.tstreams.coordination.client.TcpTransport
import com.bwsw.tstreams.generator.ITransactionGenerator

import scala.language.existentials

/**
  * @param transactionTtlMs       Single transaction live time
  * @param transactionKeepAliveMs Update transaction interval which is used to keep alive transaction in time when it is opened
  * @param writePolicy            Strategy for selecting next partition
  * @param batchSize              Insertion Type (only BatchInsert and SingleElementInsert are allowed now)
  * @param transactionGenerator   Generator for generating IDs
  */
class ProducerOptions(val transactionTtlMs: Long,
                      val transactionKeepAliveMs: Int,
                      val writePolicy: AbstractPolicy,
                      val batchSize: Int,
                      val transactionGenerator: ITransactionGenerator,
                      val coordinationOptions: CoordinationOptions) {

  /**
    * Transaction minimum ttl time
    */
  private val minTransactionTTL = 3

  /**
    * Options validating
    */
  if (transactionTtlMs < minTransactionTTL)
    throw new IllegalArgumentException(s"Option transactionTTL must be greater or equal than $minTransactionTTL")

  if (transactionKeepAliveMs < 1)
    throw new IllegalArgumentException(s"Option transactionKeepAliveInterval must be greater or equal than 1")

  if (transactionKeepAliveMs.toDouble > transactionTtlMs.toDouble / 3.0)
    throw new IllegalArgumentException("Option transactionTTL must be at least three times greater or equal than transaction")

  if (batchSize <= 0)
    throw new IllegalArgumentException("Batch size must be greater or equal 1")

}


/**
  * @param zkEndpoints        Zk hosts to connect
  * @param zkPrefix           Zk root path for all metadata
  * @param zkSessionTimeoutMs Zk session timeout
  * @param transport          Transport providing interaction between agents
  * @param threadPoolSize     Thread pool amount which is used by
  *                           PeerAgent
  *                           by default (threads_amount == used_producer_partitions)
  */
class CoordinationOptions(val zkEndpoints: String,
                          val zkPrefix: String,
                          val zkSessionTimeoutMs: Int,
                          val zkConnectionTimeoutMs: Int,
                          val transport: TcpTransport,
                          val threadPoolSize: Int,
                          val notifyThreadPoolSize: Int)