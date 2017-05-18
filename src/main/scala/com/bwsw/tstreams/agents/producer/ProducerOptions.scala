package com.bwsw.tstreams.agents.producer

import com.bwsw.tstreams.common.AbstractPolicy
import com.bwsw.tstreams.generator.ITransactionGenerator

import scala.language.existentials

class ProducerOptions(val transactionTtlMs: Long,
                      val transactionKeepAliveMs: Int,
                      val writePolicy: AbstractPolicy,
                      val batchSize: Int,
                      val transactionGenerator: ITransactionGenerator,
                      val notifyJobsThreadPoolSize: Int,
                      val coordinationOptions: CoordinationOptions)


class CoordinationOptions(val zkEndpoints: String,
                          val zkPrefix: String,
                          val zkSessionTimeoutMs: Int,
                          val zkConnectionTimeoutMs: Int,
                          val zkRetryDelayMs: Int,
                          val zkRetryCount: Int,
                          val openerServerHost: String,
                          val openerServerPort: Int,
                          val threadPoolSize: Int,
                          val transportClientTimeoutMs: Int)
