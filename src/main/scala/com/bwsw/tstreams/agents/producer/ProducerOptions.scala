package com.bwsw.tstreams.agents.producer

import com.bwsw.tstreams.common.AbstractPolicy

import scala.language.existentials

class ProducerOptions(val transactionTtlMs: Long,
                      val transactionKeepAliveMs: Int,
                      val writePolicy: AbstractPolicy,
                      val batchSize: Int,
                      val notifyJobsThreadPoolSize: Int)

