package com.bwsw.tstreams.agents.producer

import com.bwsw.tstreams.common.PartitionIterationPolicy

import scala.language.existentials

class ProducerOptions(val transactionTtlMs: Long,
                      val transactionKeepAliveMs: Int,
                      val writePolicy: PartitionIterationPolicy,
                      val batchSize: Int,
                      val notifyJobsThreadPoolSize: Int)

