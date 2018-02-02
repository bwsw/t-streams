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

package com.bwsw.tstreamstransactionserver.netty.server.compaction

import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.netty.server.storage.rocks.RocksCompactionJob
import org.mockito.Mockito.{times, verify}
import org.rocksdb.{ColumnFamilyHandle, RocksDB}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

/** Tests for [[RocksCompactionJob]]
  *
  * @author Pavel Tomskikh
  */
class RocksCompactionJobTest extends FlatSpec with Matchers with MockitoSugar {

  private val intervalMillis = 100
  private val cycles = 5

  private trait Mocks {
    val rocksDB: RocksDB = mock[RocksDB]
    val handlersCount = 5
    val handlers: Seq[ColumnFamilyHandle] = Seq.fill(handlersCount)(mock[ColumnFamilyHandle])
  }

  "CompactionJob" should "work properly" in new Mocks {
    val compactionJob = new RocksCompactionJob(rocksDB, handlers, intervalMillis, TimeUnit.MILLISECONDS)

    compactionJob.start()
    Thread.sleep(intervalMillis * cycles + intervalMillis / 2)
    compactionJob.stop()

    verify(rocksDB, times(cycles)).compactRange()
    handlers.foreach { handler =>
      verify(rocksDB, times(cycles)).compactRange(handler)
    }
  }
}
