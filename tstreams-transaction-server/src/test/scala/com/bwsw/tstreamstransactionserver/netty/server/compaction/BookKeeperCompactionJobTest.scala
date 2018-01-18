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

import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.mockito.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.{BookKeeperCompactionJob, LedgerHandle}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.hierarchy.LongZookeeperTreeList
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.storage.BookKeeperWrapper
import org.mockito.ArgumentMatchers.anyInt

import scala.util.Random

/**
  * Tests for [[BookKeeperCompactionJob]]
  */
class BookKeeperCompactionJobTest extends FlatSpec with Matchers with MockitoSugar {

  private val intervalMillis = 100
  private val intervalSeconds = 2
  private val cycles = 5
  private val waitTimeMillis = intervalMillis * cycles + intervalMillis / 2
  private val waitTimeSeconds = intervalSeconds * 1000 * cycles + intervalSeconds * 1000 / 2

  private trait Mocks {
    val treeList: LongZookeeperTreeList = mock[LongZookeeperTreeList]
    val bk: BookKeeperWrapper = mock[BookKeeperWrapper]
    val ledger: LedgerHandle = mock[LedgerHandle]
  }

  "CompactionJob" should "work properly if there are no ledgers" in new Mocks {
    val ttl: Int = -1
    when(treeList.firstEntityId).thenReturn(None)
    val compactionJob = new BookKeeperCompactionJob(Array(treeList), bk, ttl, intervalMillis, TimeUnit.MILLISECONDS)

    compactionJob.start()
    Thread.sleep(waitTimeMillis)
    compactionJob.close()

    verify(treeList, times(cycles)).firstEntityId
    verify(treeList, never()).deleteNode(anyInt())
  }

  it should "work properly if there is one ledger that isn't expired" in new Mocks {
    val ts: Long = System.currentTimeMillis()
    val ttl: Long = waitTimeMillis * 2
    val id: Long = Random.nextInt(10)
    when(ledger.getCreationTime).thenReturn(ts)
    when(bk.openLedger(id)).thenReturn(Some(ledger), None)
    when(treeList.firstEntityId).thenReturn(Some(id), None)
    val compactionJob = new BookKeeperCompactionJob(Array(treeList), bk, ttl, intervalMillis, TimeUnit.MILLISECONDS)

    compactionJob.start()
    Thread.sleep(waitTimeMillis)
    compactionJob.close()

    verify(treeList, times(cycles)).firstEntityId
    verify(bk, times(1)).openLedger(id)
    verify(treeList, never()).deleteNode(anyInt())
    verify(bk, never()).deleteLedger(anyInt())
  }

  it should "work properly if there is one ledger that is expired" in new Mocks {
    val numberOfLedgers = 1
    val ts: Long = System.currentTimeMillis()
    val ttl: Long = intervalSeconds / 2
    val id: Long = Random.nextInt(10)
    when(ledger.getCreationTime).thenReturn(ts)
    when(bk.openLedger(id)).thenReturn(Some(ledger), None)
    when(treeList.firstEntityId).thenReturn(Some(id), None)
    val compactionJob = new BookKeeperCompactionJob(Array(treeList), bk, ttl, intervalSeconds, TimeUnit.SECONDS)

    compactionJob.start()
    Thread.sleep(waitTimeSeconds)
    compactionJob.close()

    verify(treeList, times(cycles + numberOfLedgers)).firstEntityId
    verify(bk, times(1)).openLedger(id)
    verify(treeList, times(1)).deleteNode(id)
    verify(bk, times(1)).deleteLedger(id)
  }

  it should "work properly if there are more than one ledgers that are expired" in new Mocks {
    val numberOfLedgers = 2
    val ts: Long = System.currentTimeMillis()
    val ttl: Long = intervalSeconds / 2
    val firstId: Long = Random.nextInt(10)
    val secondId: Long = firstId + 1
    when(ledger.getCreationTime).thenReturn(ts)
    when(bk.openLedger(firstId)).thenReturn(Some(ledger), None)
    when(bk.openLedger(secondId)).thenReturn(Some(ledger), None)
    when(treeList.firstEntityId).thenReturn(Some(firstId), Some(secondId), None)
    val compactionJob = new BookKeeperCompactionJob(Array(treeList), bk, ttl, intervalSeconds, TimeUnit.SECONDS)

    compactionJob.start()
    Thread.sleep(waitTimeSeconds)
    compactionJob.close()

    verify(treeList, times(cycles + numberOfLedgers)).firstEntityId
    verify(bk, times(1)).openLedger(firstId)
    verify(treeList, times(1)).deleteNode(firstId)
    verify(bk, times(1)).deleteLedger(firstId)
    verify(bk, times(1)).openLedger(secondId)
    verify(treeList, times(1)).deleteNode(secondId)
    verify(bk, times(1)).deleteLedger(secondId)
  }
}
