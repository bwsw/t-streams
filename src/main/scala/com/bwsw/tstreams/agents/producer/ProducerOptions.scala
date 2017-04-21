package com.bwsw.tstreams.agents.producer

import com.bwsw.tstreams.common.AbstractPolicy
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


class CoordinationOptions(val zkEndpoints: String,
                          val zkPrefix: String,
                          val zkSessionTimeoutMs: Int,
                          val zkConnectionTimeoutMs: Int,
                          val openerServerHost: String,
                          val openerServerPort: Int,
                          val threadPoolSize: Int,
                          val notifyThreadPoolSize: Int,
                          val transportClientTimeoutMs: Int,
                          val transportClientRetryCount: Int,
                          val transportClientRetryDelayMs: Int)