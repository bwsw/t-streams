package com.bwsw.tstreams.env

import scala.collection.mutable

/**
  * Created by Ivan Kudryavtsev on 18.02.17.
  */
object TStreamsFactoryStorageDefaults {

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
    val co = ConfigurationOptions

    m(co.StorageClient.threadPool)          = StorageClient.threadPool.default
    m(co.StorageClient.connectionTimeoutMs) = StorageClient.connectionTimeoutMs.default
    m(co.StorageClient.retryDelayMs)        = StorageClient.retryDelayMs.default

    m(co.StorageClient.Auth.key)                      = StorageClient.Auth.key
    m(co.StorageClient.Auth.connectionTimeoutMs)      = StorageClient.Auth.connectionTimeoutMs.default
    m(co.StorageClient.Auth.retryDelayMs)             = StorageClient.Auth.retryDelayMs.default
    m(co.StorageClient.Auth.tokenConnectionTimeoutMs) = StorageClient.Auth.tokenConnectionTimeoutMs.default
    m(co.StorageClient.Auth.tokenRetryDelayMs)        = StorageClient.Auth.tokenRetryDelayMs.default

    m(co.StorageClient.Zookeeper.endpoints)           = StorageClient.Zookeeper.endpoints
    m(co.StorageClient.Zookeeper.prefix)              = StorageClient.Zookeeper.prefix
    m(co.StorageClient.Zookeeper.connectionTimeoutMs) = StorageClient.Zookeeper.connectionTimeoutMs.default
    m(co.StorageClient.Zookeeper.sessionTimeoutMs)    = StorageClient.Zookeeper.sessionTimeoutMs.default
    m(co.StorageClient.Zookeeper.retryCount)          = StorageClient.Zookeeper.retryCount.default
    m(co.StorageClient.Zookeeper.retryDelayMs)        = StorageClient.Zookeeper.retryDelayMs.default

    m
  }
}
