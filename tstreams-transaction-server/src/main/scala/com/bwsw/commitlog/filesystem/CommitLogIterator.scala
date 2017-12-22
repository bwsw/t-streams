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
package com.bwsw.commitlog.filesystem

import java.io.BufferedInputStream

import com.bwsw.commitlog.filesystem.CommitLogIterator.EOF
import com.bwsw.commitlog.{CommitLogRecord, CommitLogRecordHeader}

private object CommitLogIterator {
  val EOF: Int = -1
}

abstract class CommitLogIterator
  extends Iterator[Either[NoSuchElementException, CommitLogRecord]] {

  protected val stream: BufferedInputStream
  def close(): Unit = {
    stream.close()
  }

  override def next(): Either[NoSuchElementException, CommitLogRecord] = {
    if (!hasNext()) Left(new NoSuchElementException("There is no next commit log record!"))
    else {
      val recordWithoutMessage = new Array[Byte](CommitLogRecord.headerSize)
      var byte = stream.read(recordWithoutMessage)
      if (byte != EOF && byte == CommitLogRecord.headerSize) {
        val header = CommitLogRecordHeader.fromByteArray(recordWithoutMessage)
        val message = new Array[Byte](header.messageLength)
        byte = stream.read(message)
        if (byte != EOF && byte == header.messageLength) {
          Right(CommitLogRecord(header.messageType, message, header.timestamp))
        } else {
          Left(new NoSuchElementException("There is no next commit log record!"))
        }
      } else {
        Left(new NoSuchElementException("There is no next commit log record!"))
      }
    }
  }

  override def hasNext(): Boolean = {
    if (stream.available() > 0) true
    else false
  }
}
