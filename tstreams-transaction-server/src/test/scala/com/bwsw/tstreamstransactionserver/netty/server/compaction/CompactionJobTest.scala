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

import com.bwsw.tstreamstransactionserver.netty.server.CompactionJob
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

/** Tests for [[CompactionJob]]
  *
  * @author Pavel Tomskikh
  */
class CompactionJobTest extends FlatSpec with Matchers with MockitoSugar {

  private val intervalMillis = 100

  private def createCompactionJob(interval: Long): CompactionJob = new CompactionJob(interval, TimeUnit.MILLISECONDS) {
      override def compact(): Unit = {}
    }

  it should "throw an IllegalStateException by start() if it already started" in {
    val compactionJob = createCompactionJob(intervalMillis)

    compactionJob.start()
    an[IllegalStateException] shouldBe thrownBy {
      compactionJob.start()
    }
    compactionJob.close()
  }

  it should "throw an IllegalStateException by start() if it already closed" in {
    val compactionJob = createCompactionJob(intervalMillis)

    compactionJob.start()
    compactionJob.close()
    an[IllegalStateException] shouldBe thrownBy {
      compactionJob.start()
    }
  }

  it should "not be created if interval <= 0" in {
    Seq[Long](0, -1, -1000).foreach { interval =>
      an[IllegalArgumentException] shouldBe thrownBy {
        createCompactionJob(interval)
      }
    }
  }
}