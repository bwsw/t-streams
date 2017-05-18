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
    val bindPort = PortRange(20000, 60000)
    val openTimeoutMs = IntMinMaxDefault(1000, 10000, 5000)
    val threadPoolSize = IntMinMaxDefault(1, 1000, Runtime.getRuntime().availableProcessors())
    val notifyJobsThreadPoolSize = IntMinMaxDefault(1, 32, 1)

    object Transaction {
      val ttlMs = IntMinMaxDefault(500, 300000, 60000)
      val keepAliveMs = IntMinMaxDefault(100, 2000, 2000)
      val batchSize = IntMinMaxDefault(1, 1000, 100)
      val distributionPolicy = ConfigurationOptions.Producer.Transaction.Consts.DISTRIBUTION_POLICY_RR
    }

  }

  def get = {
    val m = mutable.HashMap[String, Any]()
    val co = ConfigurationOptions.Producer

    m(co.bindHost) = Producer.bindHost
    m(co.bindPort) = Producer.bindPort
    m(co.openTimeoutMs) = Producer.openTimeoutMs.default
    m(co.threadPoolSize) = Producer.threadPoolSize.default
    m(co.notifyJobsThreadPoolSize) = Producer.notifyJobsThreadPoolSize.default
    m(co.Transaction.ttlMs) = Producer.Transaction.ttlMs.default
    m(co.Transaction.keepAliveMs) = Producer.Transaction.keepAliveMs.default
    m(co.Transaction.batchSize) = Producer.Transaction.batchSize.default
    m(co.Transaction.distributionPolicy) = Producer.Transaction.distributionPolicy

    m
  }

}


