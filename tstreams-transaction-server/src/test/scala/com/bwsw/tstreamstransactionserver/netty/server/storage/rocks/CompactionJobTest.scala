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

package com.bwsw.tstreamstransactionserver.netty.server.storage.rocks

import java.util.concurrent.TimeUnit

import org.mockito.Mockito.{times, verify}
import org.rocksdb.{ColumnFamilyHandle, RocksDB}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

/** Tests for [[CompactionJob]]
  *
  * @author Pavel Tomskikh
  */
class CompactionJobTest extends FlatSpec with Matchers with MockitoSugar {

  private val intervalMillis = 100
  private val cycles = 5

  private trait Mocks {
    val rocksDB: RocksDB = mock[RocksDB]
    val handlersCount = 5
    val handlers: Seq[ColumnFamilyHandle] = Seq.fill(handlersCount)(mock[ColumnFamilyHandle])
  }

  "CompactionJob" should "work properly" in new Mocks {
    val compactionJob = new CompactionJob(rocksDB, handlers, intervalMillis, TimeUnit.MILLISECONDS)

    compactionJob.start()
    Thread.sleep(intervalMillis * cycles + intervalMillis / 2)
    compactionJob.close()

    verify(rocksDB, times(cycles)).compactRange()
    handlers.foreach { handler =>
      verify(rocksDB, times(cycles)).compactRange(handler)
    }
  }

  it should "throw an IllegalStateException by start() if it already started" in new Mocks {
    val compactionJob = new CompactionJob(rocksDB, handlers, intervalMillis, TimeUnit.MILLISECONDS)

    compactionJob.start()
    an[IllegalStateException] shouldBe thrownBy {
      compactionJob.start()
    }
    compactionJob.close()
  }

  it should "throw an IllegalStateException by start() if it already closed" in new Mocks {
    val compactionJob = new CompactionJob(rocksDB, handlers, intervalMillis, TimeUnit.MILLISECONDS)

    compactionJob.start()
    compactionJob.close()
    an[IllegalStateException] shouldBe thrownBy {
      compactionJob.start()
    }
  }

  it should "not be created if interval <= 0" in new Mocks {
    Seq[Long](0, -1, -1000).foreach { interval =>
      an[IllegalArgumentException] shouldBe thrownBy {
        new CompactionJob(rocksDB, handlers, interval, TimeUnit.MILLISECONDS)
      }
    }
  }
}
