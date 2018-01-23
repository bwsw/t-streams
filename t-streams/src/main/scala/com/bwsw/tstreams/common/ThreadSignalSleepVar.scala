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

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

/**
  * Created by Ivan Kudryavtsev on 25.07.16.
  * Allows wait for external event in thread for specified amount of time (combines sleep and flag)
  */
class ThreadSignalSleepVar[T](size: Int = 1) {
  val signalQ = new LinkedBlockingQueue[T](size)

  /**
    * waits for event for specified amount of time
    *
    * @param timeout timeout to wait
    * @param unit    TimeUnit information
    * @return
    */
  def wait(timeout: Int, unit: TimeUnit = TimeUnit.MILLISECONDS): T = signalQ.poll(timeout, unit)

  /**
    * signals about new event
    *
    * @param value
    */
  def signal(value: T) = signalQ.put(value)
}