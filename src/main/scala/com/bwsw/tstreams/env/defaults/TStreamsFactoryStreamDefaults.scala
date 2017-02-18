package com.bwsw.tstreams.env.defaults

import com.bwsw.tstreams.common.IntMinMaxDefault
import com.bwsw.tstreams.env.ConfigurationOptions

import scala.collection.mutable

/**
  * Created by Ivan Kudryavtsev on 18.02.17.
  */
object TStreamsFactoryStreamDefaults {

  object Stream {
    val name            = "test"
    val description     = ""
    val partitionsCount = IntMinMaxDefault(1, 2147483647, 1)
    val ttlSec             = IntMinMaxDefault(60, 3600 * 24 * 365 * 50 /* 50 years */, 60 * 60 * 24 /* one day */)
  }

  def get = {
    val m = mutable.HashMap[String, Any]()
    val co = ConfigurationOptions.Stream
    m(co.name) = Stream.name
    m(co.description) = ""
    m(co.partitionsCount) = Stream.partitionsCount.default
    m(co.ttlSec)             = Stream.ttlSec.default
    m
  }
}


//  // stream scope
//  propertyMap += (co.Stream.name -> "test")
//
//  val Stream_partitions_default = 1
//  val Stream_partitions_min = 1
//  val Stream_partitions_max = 100000000
//  propertyMap += (co.Stream.partitionsCount -> Stream_partitions_default)
//
//  val Stream_ttl_default = 60 * 60 * 24
//  val Stream_ttl_min = 60
//  val Stream_ttl_max = 315360000
//  propertyMap += (co.Stream.TTL -> Stream_ttl_default)
//  propertyMap += (co.Stream.description -> "Test stream")
