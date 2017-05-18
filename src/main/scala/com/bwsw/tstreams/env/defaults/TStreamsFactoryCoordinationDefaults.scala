package com.bwsw.tstreams.env.defaults

import com.bwsw.tstreams.common.IntMinMaxDefault
import com.bwsw.tstreams.env.ConfigurationOptions

import scala.collection.mutable

/**
  * Created by Ivan Kudryavtsev on 18.02.17.
  */
object TStreamsFactoryCoordinationDefaults {

  object Coordination {
    val endpoints = "localhost:2181"
    val prefix = "/t-streams"
    val sessionTimeoutMs = IntMinMaxDefault(1000, 10000, 5000)
    val connectionTimeoutMs = IntMinMaxDefault(1000, 10000, 5000)
    val retryDelayMs = IntMinMaxDefault(50, 2000, 1000)
    val retryCount = IntMinMaxDefault(1, 29, 10)
  }

  def get = {
    val m = mutable.HashMap[String, Any]()
    val co = ConfigurationOptions.Coordination

    m(co.endpoints) = Coordination.endpoints
    m(co.prefix) = Coordination.prefix
    m(co.sessionTimeoutMs) = Coordination.sessionTimeoutMs.default
    m(co.connectionTimeoutMs) = Coordination.connectionTimeoutMs.default
    m(co.retryDelayMs) = Coordination.retryDelayMs.default
    m(co.retryCount) = Coordination.retryCount.default

    m
  }
}


