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
package com.bwsw.tstreamstransactionserver.netty.client


import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.exception.Throwable.RequestTimeoutException
import io.netty.util.{HashedWheelTimer, Timeout}
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.concurrent.{ExecutionContext, Future => ScalaFuture, Promise => ScalaPromise}


object TimeoutScheduler {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val timer = new HashedWheelTimer(10, TimeUnit.MILLISECONDS)

  def withTimeout[T](fut: ScalaFuture[T])(after: Duration,
                                          requestID: Long)(implicit ec: ExecutionContext): ScalaFuture[T] = {
    val promise = ScalaPromise[T]()

    val timeout = TimeoutScheduler
      .scheduleTimeout(promise, after, requestID)

    val combinedFut =
      ScalaFuture.firstCompletedOf(
        collection.immutable.Seq(
          fut,
          promise.future
        )
      )

    fut onComplete (_ => timeout.cancel())
    combinedFut
  }

  def scheduleTimeout(promise: ScalaPromise[_],
                      after: Duration,
                      requestID: Long): Timeout = {
    timer.newTimeout(_ => {
      val requestTimeoutException =
        new RequestTimeoutException(requestID, after.toMillis)
      val isExpired =
        promise.tryFailure(requestTimeoutException)
      if (isExpired && logger.isDebugEnabled)
        logger.debug(requestTimeoutException.getMessage)
    }, after.toNanos, TimeUnit.NANOSECONDS)
  }
}
