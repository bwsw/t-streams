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
    val prefix    = "/t-streams"
    val sessionTimeoutMs                = IntMinMaxDefault(1000, 10000, 5000)
    val connectionTimeoutMs             = IntMinMaxDefault(1000, 10000, 5000)
    val partitionsRedistributionDelaySec = IntMinMaxDefault(1, 100, 2)
  }

  def get = {
    val m = mutable.HashMap[String, Any]()
    val co = ConfigurationOptions.Coordination

    m(co.endpoints) = Coordination.endpoints
    m(co.prefix)    = Coordination.prefix
    m(co.sessionTimeoutMs)                 = Coordination.sessionTimeoutMs.default
    m(co.connectionTimeoutMs)              = Coordination.connectionTimeoutMs.default

    // TODO: check deprecated
    m(co.partitionsRedistributionDelaySec) = Coordination.partitionsRedistributionDelaySec.default

    m
  }
}


