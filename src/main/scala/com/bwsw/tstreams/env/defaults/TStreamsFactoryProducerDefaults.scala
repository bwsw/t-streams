package com.bwsw.tstreams.env.defaults

import com.bwsw.tstreams.common.IntMinMaxDefault
import com.bwsw.tstreams.env.ConfigurationOptions

import scala.collection.mutable

/**
  * Created by Ivan Kudryavtsev on 18.02.17.
  */
object TStreamsFactoryProducerDefaults {

  case class PortRange(from: Int, to: Int)

  object Producer {
    val bindHost = "localhost"
    val bindPort = PortRange(40000, 50000)
    val transportTimeoutMs = IntMinMaxDefault(1000, 10000, 5000)
    val transportRetryDelayMs = IntMinMaxDefault(1000, 5000, 1000)
    val transportRetryCount = IntMinMaxDefault(3, 100, 3)
    val threadPoolSize = IntMinMaxDefault(1, 1000, Runtime.getRuntime().availableProcessors())
    val notifyJobsThreadPoolSize = IntMinMaxDefault(1, 32, 1)

    object Transaction {
      val ttlMs = IntMinMaxDefault(500, 120000, 30000)
      val openMaxWaitMs = IntMinMaxDefault(1000, 10000, 1000)
      val keepAliveMs = IntMinMaxDefault(100, 2000, 1000)
      val batchSize = IntMinMaxDefault(1, 1000, 100)
      val distributionPolicy = ConfigurationOptions.Producer.Transaction.Consts.DISTRIBUTION_POLICY_RR
    }

  }

  def get = {
    val m = mutable.HashMap[String, Any]()
    val co = ConfigurationOptions.Producer

    m(co.bindHost) = Producer.bindHost
    m(co.bindPort) = Producer.bindPort
    m(co.transportTimeoutMs) = Producer.transportTimeoutMs.default
    m(co.transportRetryCount) = Producer.transportRetryCount.default
    m(co.transportRetryDelayMs) = Producer.transportRetryDelayMs.default
    m(co.threadPoolSize) = Producer.threadPoolSize.default
    m(co.notifyJobsThreadPoolSize) = Producer.notifyJobsThreadPoolSize.default
    m(co.Transaction.ttlMs) = Producer.Transaction.ttlMs.default
    m(co.Transaction.openMaxWaitMs) = Producer.Transaction.openMaxWaitMs.default
    m(co.Transaction.keepAliveMs) = Producer.Transaction.keepAliveMs.default
    m(co.Transaction.batchSize) = Producer.Transaction.batchSize.default
    m(co.Transaction.distributionPolicy) = Producer.Transaction.distributionPolicy

    m
  }

}


