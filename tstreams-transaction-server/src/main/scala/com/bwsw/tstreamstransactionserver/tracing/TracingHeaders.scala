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

import java.nio.ByteBuffer

import zipkin2.Span

import scala.util.Random

/** Contains an information about tracing
  *
  * @param traceId    trace ID
  * @param requestId  request span ID
  * @param responseId response span ID
  * @author Pavel Tomskikh
  */
case class TracingHeaders(traceId: Long = Random.nextLong(),
                          requestId: Long = Random.nextLong(),
                          responseId: Long = Random.nextLong()) {

  /** Serializes self into byte array
    *
    * @return serialized tracing headers
    */
  def toByteArray: Array[Byte] = {
    ByteBuffer.allocate(TracingHeaders.sizeInBytes)
      .putLong(traceId)
      .putLong(requestId)
      .putLong(responseId)
      .array()
  }

  /** Returns builder for request span
    *
    * @return builder for request span
    */
  def requestSpanBuilder: Span.Builder = {
    Span.newBuilder()
      .traceId(traceId.toHexString)
      .id(requestId.toHexString)
  }

  /** Returns builder for response span
    *
    * @return builder for response span
    */
  def responseSpanBuilder: Span.Builder = {
    Span.newBuilder()
      .traceId(traceId.toHexString)
      .id(responseId.toHexString)
  }
}

object TracingHeaders {

  val sizeInBytes: Int = java.lang.Long.BYTES * 3

  /** Deserializes tracing headers from byte array
    *
    * @param bytes serialized tracing headers
    * @return tracing headers
    */
  def fromByteArray(bytes: Array[Byte]): TracingHeaders = {
    val buffer = ByteBuffer.wrap(bytes)
    val traceId = buffer.getLong
    val requestId = buffer.getLong
    val responseId = buffer.getLong

    new TracingHeaders(traceId, requestId, responseId)
  }
}