package com.bwsw.tstreams.env.defaults

import com.bwsw.tstreams.common.IntMinMaxDefault
import com.bwsw.tstreams.env.ConfigurationOptions

import scala.collection.mutable

/**
  * Created by Ivan Kudryavtsev on 18.02.17.
  */
object TStreamsFactoryStreamDefaults {

  object Stream {
    val name = "test"
    val description = ""
    val partitionsCount = IntMinMaxDefault(1, 2147483647, 1)
    val ttlSec = IntMinMaxDefault(60, 3600 * 24 * 365 * 50 /* 50 years */ , 60 * 60 * 24 /* one day */)
  }

  def get = {
    val m = mutable.HashMap[String, Any]()
    val co = ConfigurationOptions.Stream

    m(co.name) = Stream.name
    m(co.description) = ""
    m(co.partitionsCount) = Stream.partitionsCount.default
    m(co.ttlSec) = Stream.ttlSec.default

    m
  }
}


