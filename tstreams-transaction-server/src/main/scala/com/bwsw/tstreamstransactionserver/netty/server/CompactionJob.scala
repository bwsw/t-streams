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
package com.bwsw.tstreamstransactionserver.netty.server

import java.io.Closeable
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.TimeUnit
import scala.util.Try

/**
  * Periodically runs compaction process.
  * What exactly compaction means depends on context (custom logic)
  *
  * @param interval time interval between compactions
  * @param timeUnit time unit of time interval
  */

abstract class CompactionJob(interval: Long, timeUnit: TimeUnit) extends Closeable {

  require(interval > 0, "A parameter 'interval' in CompactionJob should be positive")

  private val isStarted = new AtomicBoolean(false)
  private val isClosed = new AtomicBoolean(false)
  private val executor = Executors.newScheduledThreadPool(0)
  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Method which is called at each start. Implement custom block code here
    */
  def compact(): Unit

  /**
    * Starts periodically compactions
    *
    * @throws IllegalStateException if this job already started or closed
    */
  def start(): Unit = {
    if (isClosed.get()) {
      throw new IllegalStateException("Cannot start already closed job.")
    } else if (!isStarted.getAndSet(true)) {
      executor.scheduleWithFixedDelay(
        () => compact(),
        interval,
        interval,
        timeUnit)
    } else {
      throw new IllegalStateException("Cannot start already started job.")
    }
  }

  /**
    * Stops compaction process
    */
  override def close(): Unit = {
    if (!isClosed.getAndSet(true)) {
      isStarted.set(false)
      executor.shutdown()
    }
  }
}
