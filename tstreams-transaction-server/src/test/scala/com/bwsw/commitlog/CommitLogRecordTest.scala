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

package com.bwsw.commitlog

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalatest.{FlatSpec, Matchers}

class CommitLogRecordTest extends FlatSpec with Matchers {

  "CommitLogRecord" should "be serialized/deserialized" in {
    val record1 = CommitLogRecord(0, System.currentTimeMillis(), 12345, "test_data".getBytes())
    CommitLogRecord.fromByteArray(record1.toByteArray).right.get shouldBe record1

    val record2 = CommitLogRecord(-5, System.currentTimeMillis(), 762234, "".getBytes())
    CommitLogRecord.fromByteArray(record2.toByteArray).right.get shouldBe record2
  }

}
