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
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}

import com.bwsw.tstreamstransactionserver.netty.Protocol._
import zipkin2.reporter.AsyncReporter
import zipkin2.reporter.okhttp3.OkHttpSender
import zipkin2.{Endpoint, Span}

/** Asynchronous tracer.
  *
  * Takes tracing events from queue and handles it.
  *
  * @param zipkinAddress OpenZipkin address
  * @param serviceName   local endpoint name
  * @param address       local endpoint address
  * @param port          local endpoint port
  * @param threadName    thread name
  * @author Pavel Tomskikh
  */
abstract class AsyncTracer(zipkinAddress: String,
                           serviceName: String,
                           address: InetAddress,
                           port: Int,
                           threadName: String = "tracer")
  extends Thread(threadName) {

  protected val endpoint: Endpoint = Endpoint.newBuilder()
    .ip(address)
    .port(port)
    .serviceName(serviceName)
    .build()

  protected val tracedMethods: Map[Byte, String] = Set(
    OpenTransaction,
    PutTransactionData,
    PutTransactions,
    PutTransaction,
    PutConsumerCheckpoint)
    .map(method => method.methodID -> method.name)
    .toMap

  protected val tracedMethodsIds: Set[Byte] = tracedMethods.keySet
  protected val sender: OkHttpSender = OkHttpSender.create(s"http://$zipkinAddress/api/v2/spans")
  protected val reporter: AsyncReporter[Span] = AsyncReporter.create(sender)
  protected val running: AtomicBoolean = new AtomicBoolean(true)
  protected val events: BlockingQueue[AsyncTracer.Event] = new LinkedBlockingQueue[AsyncTracer.Event]
  protected val pollingTimeout: Long = 1000


  def close(): Unit = {
    running.set(false)
    reporter.flush()
    reporter.close()
    sender.close()
  }

  override def run(): Unit = {
    while (running.get()) {
      Option(events.poll(pollingTimeout, TimeUnit.MILLISECONDS))
        .foreach(_.handle())
    }
  }

  override def interrupt(): Unit = running.set(true)
}

object AsyncTracer {

  /** Tracing event */
  trait Event {
    def handle(): Unit
  }

}