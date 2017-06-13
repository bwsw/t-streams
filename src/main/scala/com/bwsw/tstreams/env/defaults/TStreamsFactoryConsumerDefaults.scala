/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
    val transactionPreload = IntMinMaxDefault(100, 10000, 1000)
    val dataPreload = IntMinMaxDefault(10, 10000, 1000)

    object Subscriber {
      val bindHost = "localhost"
      val bindPort = PortRange(40000, 50000)
      val transactionBufferThreadPoolSize = IntMinMaxDefault(1, 64, 4)
      val processingEnginesThreadPoolSize = IntMinMaxDefault(1, 64, 1)
      val pollingFrequencyDelayMs = IntMinMaxDefault(10, 100000, 1000)
      val transactionQueueMaxLengthThreshold = IntMinMaxDefault(100, 10000000, 100000)
    }

  }

  def get = {
    val m = mutable.HashMap[String, Any]()
    val co = ConfigurationOptions.Consumer
    m(co.transactionPreload) = Consumer.transactionPreload.default
    m(co.dataPreload) = Consumer.dataPreload.default
    m(co.Subscriber.bindHost) = Consumer.Subscriber.bindHost
    m(co.Subscriber.bindPort) = Consumer.Subscriber.bindPort
    m(co.Subscriber.transactionBufferThreadPoolSize) = Consumer.Subscriber.transactionBufferThreadPoolSize.default
    m(co.Subscriber.processingEnginesThreadPoolSize) = Consumer.Subscriber.processingEnginesThreadPoolSize.default
    m(co.Subscriber.pollingFrequencyDelayMs) = Consumer.Subscriber.pollingFrequencyDelayMs.default
    m(co.Subscriber.transactionQueueMaxLengthThreshold) = Consumer.Subscriber.transactionQueueMaxLengthThreshold.default
    m
  }
}

