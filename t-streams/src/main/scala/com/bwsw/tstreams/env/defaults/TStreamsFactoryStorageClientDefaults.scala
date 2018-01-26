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
    val keepAliveIntervalMs = IntMinMaxDefault(100, 10000, 5000)
    val keepAliveThreshold = IntMinMaxDefault(1, 10, 3)
    val tracingEnabled = false
    val tracingAddress = "localhost:9411"


  }

  def get: mutable.Map[String, Any] = {
    val m = mutable.HashMap[String, Any]()
    val co = ConfigurationOptions.StorageClient

    m(co.threadPool) = StorageClient.threadPool.default
    m(co.connectionTimeoutMs) = StorageClient.connectionTimeoutMs.default
    m(co.requestTimeoutMs) = StorageClient.requestTimeoutMs.default
    m(co.retryDelayMs) = StorageClient.retryDelayMs.default
    m(co.keepAliveIntervalMs) = StorageClient.keepAliveIntervalMs.default
    m(co.keepAliveThreshold) = StorageClient.keepAliveThreshold.default
    m(co.tracingEnabled) = StorageClient.tracingEnabled
    m(co.tracingAddress) = StorageClient.tracingAddress

    m
  }
}
