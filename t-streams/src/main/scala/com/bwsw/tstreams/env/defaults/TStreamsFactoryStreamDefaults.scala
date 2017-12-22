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


