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

import org.scalatest.{FlatSpec, Matchers}

class StreamValueTest
  extends FlatSpec
    with Matchers {

  "StreamValue" should "be serialized/deserialized without description" in {
    val stream = StreamValue("streamNumber1", 10, None, Long.MaxValue, None)
    StreamValue.fromByteArray(stream.toByteArray) shouldBe stream
  }

  it should "be serialized/deserialized with negative partitions" in {
    val stream = StreamValue("streamNumber1", -5, None, Long.MaxValue, None)
    StreamValue.fromByteArray(stream.toByteArray) shouldBe stream
  }

  it should "be serialized/deserialized with negative ttl" in {
    val stream = StreamValue("streamNumber1", -5, None, Long.MinValue, None)
    StreamValue.fromByteArray(stream.toByteArray) shouldBe stream
  }

  it should "be serialized/deserialized with description" in {
    val stream = StreamValue("streamNumber1", 70, Some("test"), Long.MaxValue, None)
    StreamValue.fromByteArray(stream.toByteArray) shouldBe stream
  }

  it should "be serialized/deserialized with empty description" in {
    val stream = StreamValue("streamNumber1", 16, Some(""), Long.MaxValue, None)
    StreamValue.fromByteArray(stream.toByteArray) shouldBe stream
  }
}
