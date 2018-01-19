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

package com.bwsw.tstreamstransactionserver.netty.server.authService

import com.bwsw.tstreamstransactionserver.netty.server.batch.Frame
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.ScheduledCommitLog
import org.mockito.ArgumentMatchers
import org.mockito.ArgumentMatchers.argThat
import org.mockito.Mockito.verify
import org.scalatest.mockito.MockitoSugar
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

/** Tests for [[TokenCommitLogWriter]]
  *
  * @author Pavel Tomskikh
  */
class TokenCommitLogWriterTest
  extends FlatSpec
    with Matchers
    with MockitoSugar
    with TableDrivenPropertyChecks {

  "TokenCommitLogWriter" should "write token in CommitLog correctly" in {
    val commitLog = mock[ScheduledCommitLog]
    val tokenWriter = new TokenCommitLogWriter(commitLog)

    forAll(Table[Byte, Int => Unit](
      ("recordTypeId", "method"),
      (Frame.TokenCreatedType, tokenWriter.tokenCreated),
      (Frame.TokenUpdatedType, tokenWriter.tokenUpdated),
      (Frame.TokenExpiredType, tokenWriter.tokenExpired))) { (recordTypeId, method) =>
      val token = Random.nextInt()
      method(token)

      verify(commitLog).putData(
        ArgumentMatchers.eq(recordTypeId),
        argThat[Array[Byte]](bytes => Frame.deserializeToken(bytes) == token))
    }
  }
}
