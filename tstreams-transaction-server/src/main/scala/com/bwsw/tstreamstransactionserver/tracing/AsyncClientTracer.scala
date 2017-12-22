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
import zipkin2.Span

/** Asynchronous tracer for TTS client
  *
  * @param zipkinAddress OpenZipkin address
  * @param serviceName   TTS client name
  * @param clientAddress TTS client address
  * @param clientPort    TTS client port
  * @author Pavel Tomskikh
  */
class AsyncClientTracer(zipkinAddress: String,
                        serviceName: String,
                        clientAddress: InetAddress = InetAddress.getLocalHost,
                        clientPort: Int = 0)
  extends AsyncTracer(
    zipkinAddress,
    serviceName,
    clientAddress,
    clientPort,
    "client-tracer")
    with ClientTracer {

  override def clientSend(message: RequestMessage): RequestMessage = {
    if (tracedMethodsIds.contains(message.methodId)) {
      val tracingInfo = TracingHeaders()
      val timestamp = Clock.currentTimeMicroseconds

      events.put(ClientSend(tracingInfo, timestamp))

      message.copy(tracingInfo = Some(tracingInfo))
    } else
      message
  }

  override def clientReceive(tracingInfo: TracingHeaders): Unit = {
    val timestamp = Clock.currentTimeMicroseconds
    events.put(ClientReceive(tracingInfo, timestamp))
  }


  private def withEndpoint(builder: Span.Builder) =
    builder.kind(Span.Kind.CLIENT).localEndpoint(endpoint)


  case class ClientSend(tracingInfo: TracingHeaders, timestamp: Long) extends AsyncTracer.Event {
    override def handle(): Unit = {
      val span = withEndpoint(tracingInfo.requestSpanBuilder)
        .name("request")
        .addAnnotation(timestamp, "cs")
        .build()

      reporter.report(span)
    }
  }

  case class ClientReceive(tracingInfo: TracingHeaders, timestamp: Long) extends AsyncTracer.Event {
    override def handle(): Unit = {
      val span = withEndpoint(tracingInfo.responseSpanBuilder)
        .addAnnotation(timestamp, "cr")
        .build()

      reporter.report(span)
    }
  }

}