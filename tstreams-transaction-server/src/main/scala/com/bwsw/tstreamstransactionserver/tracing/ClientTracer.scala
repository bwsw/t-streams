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

import java.net.InetAddress

import com.bwsw.tstreamstransactionserver.netty.RequestMessage
import com.bwsw.tstreamstransactionserver.options.CommonOptions.TracingOptions

/** Provides methods to trace TTS client
  *
  * @author Pavel Tomskikh
  */
trait ClientTracer {

  /** Reports client sent request
    *
    * @param message request
    * @return request with tracing metadata
    */
  def clientSend(message: RequestMessage): RequestMessage

  /** Reports client received request
    *
    * @param tracingHeaders tracing headers
    */
  def clientReceive(tracingHeaders: TracingHeaders): Unit

  def close(): Unit
}


object ClientTracer {
  def apply(options: TracingOptions,
            serviceName: String = "TTS-client",
            clientAddress: InetAddress = InetAddress.getLocalHost,
            clientPort: Int = 0): ClientTracer = {
    if (options.enabled) {
      val tracer = new AsyncClientTracer(options.endpoint, serviceName)
      tracer.start()

      tracer
    }
    else DisabledClientTracer
  }
}