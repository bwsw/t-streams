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

/** Doesn't trace TTS server
  *
  * @author Pavel Tomskikh
  */
object DisabledServerTracer extends ServerTracer {
  override def withTracing[T](request: RequestMessage,
                              name: => String,
                              parentName: => Option[String])
                             (traced: => T): T = traced

  override def invoke(request: RequestMessage, name: => String, parentName: => Option[String]): Unit = None

  override def finish(request: RequestMessage, name: => String): Unit = {}

  override def serverSend(request: RequestMessage): Unit = {}

  override def serverReceive(request: RequestMessage): Unit = {}

  override def close(): Unit = {}
}