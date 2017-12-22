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

package com.bwsw.tstreams.common

import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Created by Ivan Kudryavtsev on 11.08.16.
  */
object TimeTracker {
  val isEnabled = true
  val trackMap = mutable.Map[String, (Long /* acc */ , Long /* first */ , Long, Long)]()
  val logger = LoggerFactory.getLogger(this.getClass)


  def update_start(key: String): Unit = if (isEnabled)
    this.synchronized {
      val opt = trackMap.get(key)
      val time = System.currentTimeMillis()
      if (opt.isDefined)
        trackMap(key) = (opt.get._1, time, opt.get._3, opt.get._4)
      else
        trackMap(key) = (0, time, 0, 0)
    }

  def update_end(key: String): Unit = if (isEnabled) {
    val opt = trackMap.get(key)
    val time = System.currentTimeMillis()
    if (opt.isEmpty)
      throw new IllegalStateException(s"Wrong usage - end before start: $key")
    val delta = if (time - opt.get._2 > 1000) 0 else time - opt.get._2
    trackMap(key) = (
      opt.get._1 + delta,
      0,
      opt.get._3 + 1,
      if (delta > opt.get._4) delta else opt.get._4)
    //dump()
  }

  def dump() = if (isEnabled)
    this.synchronized {
      trackMap.foreach(kv => logger.info(s"Metric '${kv._1}' -> Total time '${kv._2._1}', Max '${kv._2._4}', Count '${kv._2._3}'"))
    }
}
