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

package com.bwsw.tstreamstransactionserver.tracing

import com.bwsw.tstreamstransactionserver.netty.RequestMessage
import com.bwsw.tstreamstransactionserver.options.CommonOptions.TracingOptions

import scala.util.Random

/** Provides methods to trace TTS server
  *
  * @author Pavel Tomskikh
  */
trait ServerTracer {

  /** Traces some method if tracing enabled and request should be traced
    *
    * @param request    request
    * @param name       span name
    * @param parentName parent span name
    * @param traced     method to trace
    * @tparam T traced method return type
    * @return traced method result
    */
  def withTracing[T](request: RequestMessage,
                     name: => String = Random.nextLong().toHexString,
                     parentName: => Option[String] = None)
                    (traced: => T): T

  /** Reports server invoked into some method
    *
    * @param request    request
    * @param name       span name
    * @param parentName parent span name
    * @return name of created span
    */
  def invoke(request: RequestMessage,
             name: => String = Random.nextLong().toHexString,
             parentName: => Option[String] = None): Unit

  /** Reports server finished some method
    *
    * @param request request
    * @param name    span name
    */
  def finish(request: RequestMessage, name: => String): Unit

  /** Reports server sent response to a request
    *
    * @param request request
    */
  def serverSend(request: RequestMessage): Unit

  /** Reports server received request
    *
    * @param request request
    */
  def serverReceive(request: RequestMessage): Unit

  def close(): Unit
}


object ServerTracer {

  private var _tracer: ServerTracer = DisabledServerTracer
  private var initialized = false

  /** Initializes tracer
    *
    * @param options tracer options
    * @throws IllegalStateException if tracer already initialized
    */
  def init(options: TracingOptions, serviceName: String = "TTS"): Unit = {
    if (initialized) throw new IllegalStateException("Tracer already initialized")
    if (options.enabled) {
      val tracer = new AsyncServerTracer(options.endpoint, serviceName)
      tracer.start()
      _tracer = tracer
      initialized = true
    }
  }

  def tracer: ServerTracer = _tracer
}