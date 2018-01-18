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

import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{Executors, TimeUnit}

import com.bwsw.tstreamstransactionserver.exception.Throwable.KeepAliveFailedException
import com.bwsw.tstreamstransactionserver.netty.Protocol
import com.bwsw.tstreamstransactionserver.options.ClientOptions.ConnectionOptions
import com.bwsw.tstreamstransactionserver.rpc.TransactionService.KeepAlive

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Success, Try}


/** Periodically notifies server that client is alive
  *
  * @param connectionOptions connection options
  * @param client            client to send keep-alive requests
  * @param onFailure         invokes when keep-alive job failed
  * @author Pavel Tomskikh
  */
class KeepAliveJob(connectionOptions: ConnectionOptions,
                   client: InetClient,
                   onFailure: Throwable => Unit)
                  (implicit executionContext: ExecutionContext) {

  private val isRunning = new AtomicBoolean(false)
  private val failures = new AtomicInteger(0)
  private lazy val executor = Executors.newScheduledThreadPool(1)

  /** Starts keep-alive job */
  def start(): Unit = {
    if (!isRunning.getAndSet(true)) {
      executor.scheduleAtFixedRate(
        () => sendKeepAlive(),
        0,
        connectionOptions.keepAliveIntervalMs,
        TimeUnit.MILLISECONDS)
    }
  }

  /** Stops keep-alive job */
  def stop(): Unit = {
    if (isRunning.getAndSet(false)) {
      executor.shutdown()
    }
  }

  /** Sends one keep-alive request and handles response */
  private def sendKeepAlive(): Unit = {
    if (client.isConnected) {
      if (failures.get() < connectionOptions.keepAliveThreshold) {
        val response = client.method[KeepAlive.Args, KeepAlive.Result, Option[Boolean]](
          Protocol.KeepAlive,
          KeepAlive.Args(),
          _.success)

        Try(Await.result(response, connectionOptions.keepAliveIntervalMs.millis)) match {
          case Success(Some(true)) => failures.set(0)
          case _ => failures.incrementAndGet()
        }
      } else {
        val exception = new KeepAliveFailedException()
        onFailure(exception)
        stop()
      }
    } else {
      stop()
    }
  }
}
