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

import com.bwsw.commitlog.{CommitLog, CommitLogRecord}


private object CommitLogIterator {
  private val errorMessage = "There is no next commit log record!"
}

abstract class CommitLogIterator
  extends Iterator[Either[Exception, CommitLogRecord]] {

  import CommitLogIterator.errorMessage

  protected val stream: BufferedInputStream

  def close(): Unit = {
    stream.close()
  }

  override def next(): Either[Exception, CommitLogRecord] = {
    if (!hasNext()) Left(new NoSuchElementException(errorMessage))
    else {
      val lengthBytes = new Array[Byte](Integer.BYTES)
      if (stream.read(lengthBytes) == Integer.BYTES) {
        val length = CommitLog.bytesToInt(lengthBytes)
        val recordBytes = new Array[Byte](length)
        if (stream.read(recordBytes) == length) {
          CommitLogRecord.fromByteArray(recordBytes)
        } else {
          Left(new NoSuchElementException(errorMessage))
        }
      } else {
        Left(new NoSuchElementException(errorMessage))
      }
    }
  }

  override def hasNext(): Boolean = stream.available > 0
}
