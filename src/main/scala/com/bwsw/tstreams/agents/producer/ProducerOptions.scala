package com.bwsw.tstreams.agents.producer

import com.bwsw.tstreams.common.AbstractPartitionIterationPolicy

import scala.language.existentials

class ProducerOptions(val transactionTtlMs: Long,
                      val transactionKeepAliveMs: Int,
                      val writePolicy: AbstractPartitionIterationPolicy,
                      val batchSize: Int,
                      val notifyJobsThreadPoolSize: Int)

