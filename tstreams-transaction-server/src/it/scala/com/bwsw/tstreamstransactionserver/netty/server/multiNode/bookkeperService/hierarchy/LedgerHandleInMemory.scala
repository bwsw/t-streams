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

package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy

import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.LedgerHandle
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.data.{Record, RecordWithIndex}
import org.apache.bookkeeper.client.BKException.Code

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Future, Promise}
import scala.util.{Failure, Success, Try}

class LedgerHandleInMemory(id: Long,
                           time: Long)
  extends LedgerHandle(id) {
  @volatile private var isClosed = false
  private val entryIDGen = new AtomicLong(0L)
  private val storage =
    new scala.collection.concurrent.TrieMap[Long, Record]()

  override def addRecord(data: Record): Long = {
    if (isClosed)
      throw new IllegalAccessError()

    val id = entryIDGen.getAndIncrement()
    storage.put(id, data)
    id
  }

  override def asyncAddRecord[T](record: Record, callback: LedgerHandle.Callback[T], promise: Promise[T]): Unit = {
    Future {
      Try(addRecord(record)) match {
        case Success(_) => callback.addComplete(Code.OK, promise)
        case Failure(_) => callback.addComplete(Code.LedgerClosedException, promise)
      }
    }
  }

  override def getRecord(id: Long): Record =
    storage.get(id).orNull

  override def lastRecordID(): Long =
    entryIDGen.get() - 1L

  override def lastEnqueuedRecordId: Long = lastRecordID()

  override def lastRecord(): Option[Record] = {
    storage.get(lastRecordID())
  }

  override def readRecords(from: Long, to: Long): Array[Record] = {
    val fromCorrected =
      if (from < 0L)
        0L
      else
        from

    val dataNumber = scala.math.abs(to - fromCorrected + 1)
    val data = new Array[Record](dataNumber.toInt)

    var index = 0
    var toReadIndex = fromCorrected
    while (index < dataNumber) {
      data(index) = storage(toReadIndex)
      index = index + 1
      toReadIndex = toReadIndex + 1
    }
    data
  }

  override def getOrderedRecords(from: Long): Array[RecordWithIndex] = {
    val fromCorrected =
      if (from < 0L)
        0L
      else
        from

    val lastRecord = lastRecordID()
    val indexes = fromCorrected to lastRecord

    readRecords(fromCorrected, lastRecord)
      .zip(indexes).sortBy(_._1.timestamp)
      .map { case (record, index) =>
        RecordWithIndex(index, record)
      }
  }

  override def close(): Unit = {
    isClosed = true
  }

  override val getCreationTime: Long = {
    time
  }
}
