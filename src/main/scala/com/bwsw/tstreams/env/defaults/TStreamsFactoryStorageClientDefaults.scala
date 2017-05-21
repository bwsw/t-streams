package com.bwsw.tstreams.env.defaults

import com.bwsw.tstreams.common.IntMinMaxDefault
import com.bwsw.tstreams.env.ConfigurationOptions

import scala.collection.mutable

/**
  * Created by Ivan Kudryavtsev on 18.02.17.
  */
object TStreamsFactoryStorageClientDefaults {

  object StorageClient {
    val threadPool = IntMinMaxDefault(1, 32, 4)
    val connectionTimeoutMs = IntMinMaxDefault(1000, 10000, 5000)
    val requestTimeoutMs = IntMinMaxDefault(100, 5000, 5000)
    val requestTimeoutRetryCount = IntMinMaxDefault(1, 20, 5)
    val retryDelayMs = IntMinMaxDefault(50, 5000, 1000)

    object Auth {
      val key = ""
    }

    object Zookeeper {
      val prefix = "/tts/master"
    }

  }

  def get = {
    val m = mutable.HashMap[String, Any]()
    val co = ConfigurationOptions.StorageClient

    m(co.threadPool) = StorageClient.threadPool.default
    m(co.connectionTimeoutMs) = StorageClient.connectionTimeoutMs.default
    m(co.requestTimeoutMs) = StorageClient.requestTimeoutMs.default
    m(co.requestTimeoutRetryCount) = StorageClient.requestTimeoutRetryCount.default
    m(co.retryDelayMs) = StorageClient.retryDelayMs.default
    m(co.Auth.key) = StorageClient.Auth.key
    m(co.Zookeeper.prefix) = StorageClient.Zookeeper.prefix

    m
  }
}
