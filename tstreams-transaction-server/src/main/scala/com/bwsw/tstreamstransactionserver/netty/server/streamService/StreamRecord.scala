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
package com.bwsw.tstreamstransactionserver.netty.server.streamService


import com.bwsw.tstreamstransactionserver.netty.server.streamService
import com.bwsw.tstreamstransactionserver.rpc
import com.sun.xml.internal.messaging.saaj.util.ByteInputStream
import org.json4s._
import org.json4s.jackson.JsonMethods.{parse, pretty, render}

case class StreamRecord(key: StreamKey, stream: streamService.StreamValue)
  extends rpc.Stream {
  override def id: Int = key.id

  override def name: String = stream.name

  override def partitions: Int = stream.partitions

  override def ttl: Long = stream.ttl

  override def description: Option[String] = stream.description

  override def zkPath: String = stream.zkPath.get

  def toBinaryJson: Array[Byte] = {
    implicit val formats = DefaultFormats
    pretty(render(Extraction.decompose(this))).getBytes
  }
}

object StreamRecord {
  def fromBinaryJson(bytes: Array[Byte]): StreamRecord = {
    implicit val formats = DefaultFormats
    parse(new ByteInputStream(bytes, bytes.length))
      .extract[StreamRecord]
  }
}
