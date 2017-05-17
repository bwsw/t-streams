package com.bwsw.tstreams.agents.producer

import com.bwsw.tstreams.common.AbstractPolicy
import com.bwsw.tstreams.generator.ITransactionGenerator

import scala.language.existentials

/**
  *
  * @param transactionTtlMs
  * @param transactionKeepAliveMs
  * @param writePolicy
  * @param batchSize
  * @param transactionGenerator
  * @param notifyJobsThreadPoolSize
  * @param coordinationOptions
  */
class ProducerOptions(val transactionTtlMs: Long,
                      val transactionKeepAliveMs: Int,
                      val writePolicy: AbstractPolicy,
                      val batchSize: Int,
                      val transactionGenerator: ITransactionGenerator,
                      val notifyJobsThreadPoolSize: Int,
                      val coordinationOptions: CoordinationOptions)

/**
  *
  * @param zkEndpoints
  * @param zkPrefix
  * @param zkSessionTimeoutMs
  * @param zkConnectionTimeoutMs
  * @param openerServerHost
  * @param openerServerPort
  * @param threadPoolSize
  * @param transportClientTimeoutMs
  * @param transportClientRetryCount
  * @param transportClientRetryDelayMs
  */
class CoordinationOptions(val zkEndpoints: String,
                          val zkPrefix: String,
                          val zkSessionTimeoutMs: Int,
                          val zkConnectionTimeoutMs: Int,
                          val openerServerHost: String,
                          val openerServerPort: Int,
                          val threadPoolSize: Int,
                          val transportClientTimeoutMs: Int,
                          val transportClientRetryCount: Int,
                          val transportClientRetryDelayMs: Int)