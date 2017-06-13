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
object TStreamsFactoryCoordinationDefaults {

  object Coordination {
    val endpoints = "localhost:2181"
    val path = "/tts/master"
    val sessionTimeoutMs = IntMinMaxDefault(1000, 10000, 5000)
    val connectionTimeoutMs = IntMinMaxDefault(1000, 10000, 5000)
    val retryDelayMs = IntMinMaxDefault(50, 2000, 1000)
    val retryCount = IntMinMaxDefault(1, 29, 10)
  }

  def get = {
    val m = mutable.HashMap[String, Any]()
    val co = ConfigurationOptions.Coordination

    m(co.endpoints) = Coordination.endpoints
    m(co.path) = Coordination.path
    m(co.sessionTimeoutMs) = Coordination.sessionTimeoutMs.default
    m(co.connectionTimeoutMs) = Coordination.connectionTimeoutMs.default
    m(co.retryDelayMs) = Coordination.retryDelayMs.default
    m(co.retryCount) = Coordination.retryCount.default

    m
  }
}


