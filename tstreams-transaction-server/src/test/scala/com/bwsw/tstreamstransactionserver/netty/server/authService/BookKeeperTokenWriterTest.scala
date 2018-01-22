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

import com.bwsw.tstreamstransactionserver.exception.Throwable.ServerIsSlaveException
import com.bwsw.tstreamstransactionserver.netty.server.batch.Frame
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.BookkeeperMaster
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.Record
import org.apache.bookkeeper.client.LedgerHandle
import org.mockito.ArgumentMatchers.{any, argThat}
import org.mockito.Mockito.{verify, when}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.ExecutionContext
import scala.util.Random

/** Tests for [[BookKeeperTokenWriter]]
  *
  * @author Pavel Tomskikh
  */
class BookKeeperTokenWriterTest
  extends FlatSpec
    with Matchers
    with MockitoSugar
    with TableDrivenPropertyChecks {

  "TokenBookKeeperWriter" should "write token in BookKeeper correctly" in {
    val executionContext = ExecutionContext.Implicits.global
    val ledgerHandle = mock[LedgerHandle]
    val bookKeeperMaster = mock[BookkeeperMaster]

    when(bookKeeperMaster.doOperationWithCurrentWriteLedger(any()))
      .thenAnswer(invocation => {
        val method =
          invocation.getArgument[Either[ServerIsSlaveException, org.apache.bookkeeper.client.LedgerHandle] => _](0)

        method(Right(ledgerHandle))
      })

    val tokenWriter = new BookKeeperTokenWriter(bookKeeperMaster, executionContext)

    forAll(Table[Byte, Int => Unit](
      ("recordTypeId", "method"),
      (Frame.TokenCreatedType, tokenWriter.tokenCreated),
      (Frame.TokenUpdatedType, tokenWriter.tokenUpdated),
      (Frame.TokenExpiredType, tokenWriter.tokenExpired))) { (recordTypeId, method) =>
      val token = Random.nextInt()
      val startTime = System.currentTimeMillis()
      method(token)

      Thread.sleep(100) // wait until a record has been written

      verify(ledgerHandle)
        .addEntry(argThat[Array[Byte]](bytes => {
          val record = Record.fromByteArray(bytes)
          val endTime = System.currentTimeMillis()
          record.recordType == recordTypeId &&
            Frame.deserializeToken(record.body) == token &&
            record.timestamp >= startTime &&
            record.timestamp <= endTime
        }))
    }
  }
}
