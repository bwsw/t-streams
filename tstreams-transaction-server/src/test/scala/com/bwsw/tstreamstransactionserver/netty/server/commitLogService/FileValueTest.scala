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

package com.bwsw.tstreamstransactionserver.netty.server.commitLogService

import org.scalatest.{FlatSpec, Matchers}

class FileValueTest
  extends FlatSpec
    with Matchers {

  "FileValue" should "be serialized/deserialized" in {
    val fileValue1 = FileValue(new String("test_data_to_check").getBytes, None)
    val fileValue1FromByteArray = FileValue.fromByteArray(fileValue1.toByteArray)
    fileValue1 shouldBe fileValue1FromByteArray

    val fileValue2 = FileValue(new String("test_dat_to_check").getBytes, Some(Array.fill(32)(1: Byte)))
    val fileValue2FromByteArray = FileValue.fromByteArray(fileValue2.toByteArray)
    fileValue2 shouldBe fileValue2FromByteArray

    val fileValue3 = FileValue(Array(0: Byte), None)
    val fileValue3FromByteArray = FileValue.fromByteArray(fileValue3.toByteArray)
    fileValue3 shouldBe fileValue3FromByteArray
  }
}
