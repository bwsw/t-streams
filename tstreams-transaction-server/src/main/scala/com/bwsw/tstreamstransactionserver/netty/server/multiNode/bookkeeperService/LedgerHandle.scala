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

package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.data.{Record, RecordWithIndex}

import scala.concurrent.Promise

object LedgerHandle {
  val TimestampKey = "timestamp"

  trait Callback[T] {
    def addComplete(bkCode: Int, promise: Promise[T]): Unit
  }

}

abstract class LedgerHandle(val id: Long) {
  def addRecord(data: Record): Long

  def asyncAddRecord[T](record: Record, callback: LedgerHandle.Callback[T], promise: Promise[T]): Unit

  val getCreationTime: Long

  def getRecord(id: Long): Record

  def readRecords(from: Long, to: Long): Array[Record]

  def getOrderedRecords(from: Long): Array[RecordWithIndex]

  def lastRecord(): Option[Record]

  def lastRecordID(): Long

  def lastEnqueuedRecordId: Long

  def close(): Unit
}
