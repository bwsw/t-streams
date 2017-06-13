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
object TStreamsFactoryProducerDefaults {

  case class PortRange(from: Int, to: Int)

  object Producer {
    val notifyJobsThreadPoolSize = IntMinMaxDefault(1, 32, 1)

    object Transaction {
      val ttlMs = IntMinMaxDefault(500, 300000, 60000)
      val keepAliveMs = IntMinMaxDefault(100, 60000, 6000)
      val batchSize = IntMinMaxDefault(1, 1000, 100)
      val distributionPolicy = ConfigurationOptions.Producer.Transaction.Constants.DISTRIBUTION_POLICY_RR
    }

  }

  def get = {
    val m = mutable.HashMap[String, Any]()
    val co = ConfigurationOptions.Producer

    m(co.notifyJobsThreadPoolSize) = Producer.notifyJobsThreadPoolSize.default
    m(co.Transaction.ttlMs) = Producer.Transaction.ttlMs.default
    m(co.Transaction.keepAliveMs) = Producer.Transaction.keepAliveMs.default
    m(co.Transaction.batchSize) = Producer.Transaction.batchSize.default
    m(co.Transaction.distributionPolicy) = Producer.Transaction.distributionPolicy

    m
  }

}


