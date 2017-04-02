package com.bwsw.tstreams.env.defaults

import com.bwsw.tstreams.common.IntMinMaxDefault
import com.bwsw.tstreams.env.ConfigurationOptions
import com.bwsw.tstreams.env.defaults.TStreamsFactoryProducerDefaults.PortRange

import scala.collection.mutable

/**
  * Created by Ivan Kudryavtsev on 18.02.17.
  */
object TStreamsFactoryConsumerDefaults {

  object Consumer {
    val transactionPreload = IntMinMaxDefault(1, 10000, 1000)
    val dataPreload = IntMinMaxDefault(10, 200, 100)

    object Subscriber {
      val bindHost = "localhost"
      val bindPort = PortRange(40000, 50000)
      val persistentQueuePath = null
      val transactionBufferThreadPoolSize = IntMinMaxDefault(1, 64, 4)
      val processingEnginesThreadPoolSize = IntMinMaxDefault(1, 64, 1)
      val pollingFrequencyDelayMs = IntMinMaxDefault(10, 100000, 1000)
    }

  }

  def get = {
    val m = mutable.HashMap[String, Any]()
    val co = ConfigurationOptions.Consumer
    m(co.transactionPreload) = Consumer.transactionPreload.default
    m(co.dataPreload) = Consumer.dataPreload.default
    m(co.Subscriber.bindHost) = Consumer.Subscriber.bindHost
    m(co.Subscriber.bindPort) = Consumer.Subscriber.bindPort
    m(co.Subscriber.persistentQueuePath) = Consumer.Subscriber.persistentQueuePath
    m(co.Subscriber.transactionBufferThreadPoolSize) = Consumer.Subscriber.transactionBufferThreadPoolSize.default
    m(co.Subscriber.processingEnginesThreadPoolSize) = Consumer.Subscriber.processingEnginesThreadPoolSize.default
    m(co.Subscriber.pollingFrequencyDelayMs) = Consumer.Subscriber.pollingFrequencyDelayMs.default
    m
  }
}

