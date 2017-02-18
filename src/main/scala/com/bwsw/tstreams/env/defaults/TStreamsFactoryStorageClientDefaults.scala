package com.bwsw.tstreams.env.defaults

import com.bwsw.tstreams.common.IntMinMaxDefault
import com.bwsw.tstreams.env.ConfigurationOptions

import scala.collection.mutable

/**
  * Created by Ivan Kudryavtsev on 18.02.17.
  */
object TStreamsFactoryStorageClientDefaults {

  object StorageClient {
    val threadPool          = IntMinMaxDefault(1,     4,      4)
    val connectionTimeoutMs = IntMinMaxDefault(1000,  10000,  5000)
    val retryDelayMs        = IntMinMaxDefault(50,    500,    200)

    object Auth {
      val key = ""
      val connectionTimeoutMs      = IntMinMaxDefault(1000,   10000,  5000)
      val retryDelayMs             = IntMinMaxDefault(50,     1000,   500)
      val tokenConnectionTimeoutMs = IntMinMaxDefault(1000,   10000,  5000)
      val tokenRetryDelayMs        = IntMinMaxDefault(50,     500,    200)
    }

    object Zookeeper {
      val endpoints                 = "127.0.0.1:2181"
      val prefix                    = "/tts"
      val connectionTimeoutMs       = IntMinMaxDefault(1000,   100000,  5000)
      val sessionTimeoutMs          = IntMinMaxDefault(1000,   100000,  5000)
      val retryDelayMs              = IntMinMaxDefault(50,     1000,    500)
      val retryCount                = IntMinMaxDefault(3,      100,     10)
    }

  }

  def get = {
    val m = mutable.HashMap[String, Any]()
    val co = ConfigurationOptions.StorageClient

    m(co.threadPool)          = StorageClient.threadPool.default
    m(co.connectionTimeoutMs) = StorageClient.connectionTimeoutMs.default
    m(co.retryDelayMs)        = StorageClient.retryDelayMs.default

    m(co.Auth.key)                      = StorageClient.Auth.key
    m(co.Auth.connectionTimeoutMs)      = StorageClient.Auth.connectionTimeoutMs.default
    m(co.Auth.retryDelayMs)             = StorageClient.Auth.retryDelayMs.default
    m(co.Auth.tokenConnectionTimeoutMs) = StorageClient.Auth.tokenConnectionTimeoutMs.default
    m(co.Auth.tokenRetryDelayMs)        = StorageClient.Auth.tokenRetryDelayMs.default

    m(co.Zookeeper.endpoints)           = StorageClient.Zookeeper.endpoints
    m(co.Zookeeper.prefix)              = StorageClient.Zookeeper.prefix
    m(co.Zookeeper.connectionTimeoutMs) = StorageClient.Zookeeper.connectionTimeoutMs.default
    m(co.Zookeeper.sessionTimeoutMs)    = StorageClient.Zookeeper.sessionTimeoutMs.default
    m(co.Zookeeper.retryCount)          = StorageClient.Zookeeper.retryCount.default
    m(co.Zookeeper.retryDelayMs)        = StorageClient.Zookeeper.retryDelayMs.default

    m
  }
}
