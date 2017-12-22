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
package com.bwsw.commitlog

import java.io._
import java.security.{DigestOutputStream, MessageDigest}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong, AtomicReference}
import javax.xml.bind.DatatypeConverter

import com.bwsw.commitlog.CommitLogFlushPolicy.{ICommitLogFlushPolicy, OnCountInterval, OnRotation, OnTimeInterval}
import com.bwsw.commitlog.filesystem.FilePathManager


/** Logger which stores records continuously in files in specified location.
  *
  * Stores data in files placed YYYY/mm/dd/{serial number}.dat. If it works correctly, md5-files placed
  * YYYY/mm/dd/{serial number}.md5 shall be generated as well. New file starts on user request or when configured time
  * was exceeded.
  *
  * @param seconds period of time to write records into the same file, then start new file
  * @param path    location to store files at
  * @param policy  policy to flush data into file (OnRotation by default)
  */
class CommitLog(seconds: Int,
                path: String,
                policy: ICommitLogFlushPolicy = OnRotation,
                iDGenerator: IDGenerator[Long]) {
  require(seconds > 0, "Seconds cannot be less than 1")
  private val millisInterval: Long = TimeUnit.SECONDS.toMillis(seconds)

  private val chunkWriteCount: AtomicInteger = new AtomicInteger(0)
  private val chunkOpenTime: AtomicLong = new AtomicLong(0L)

  private val pathWithSeparator =
    s"$path${java.io.File.separatorChar}"

  private val currentCommitLogFileToPut =
    new AtomicReference[CommitLogFile](
      new CommitLogFile(iDGenerator.nextID)
    )

  /** Puts record and its type to an appropriate file.
    *
    * Writes data to file in format (delimiter)(BASE64-encoded type and message). When writing to one file finished,
    * md5-sum file generated.
    *
    * @param message     message to store
    * @param messageType type of message to store
    * @param startNew    start new file if true
    * @return name of file record was saved in
    */
  def putRec(message: Array[Byte], messageType: Byte, startNew: Boolean = false): String = this.synchronized {
    val now: Long = System.currentTimeMillis()
    policy match {
      case interval: OnTimeInterval if interval.seconds * 1000 + chunkOpenTime.get() < now =>
        chunkOpenTime.set(now)
        currentCommitLogFileToPut.get().flush()
      case interval: OnCountInterval if interval.count == chunkWriteCount.get() =>
        chunkWriteCount.set(0)
        currentCommitLogFileToPut.get().flush()
      case _ =>
    }

    // If we want to open new File or if time between creation new files exceeds `millisInterval`
    if (startNew || timeExceeded()) close()

    val currentFile = currentCommitLogFileToPut.get()
    currentFile.put(messageType, message)
    chunkWriteCount.incrementAndGet()

    currentFile.absolutePath
  }

  /** Finishes work with current file. */
  def close(createNewFile: Boolean = true, withMD5: Boolean = true): String = this.synchronized {
    val currentCommitLogFile = currentCommitLogFileToPut.get()

    val path = currentCommitLogFile.absolutePath
    if (createNewFile) {
      currentCommitLogFileToPut.set(
        new CommitLogFile(
          iDGenerator.nextID
        )
      )
    }
    currentCommitLogFile.close()
    resetCounters()
    path
  }

  private def resetCounters(): Unit = {
    chunkWriteCount.set(0)
    chunkOpenTime.set(System.currentTimeMillis())
  }

  private def timeExceeded(): Boolean = {
    (System.currentTimeMillis() - currentCommitLogFileToPut.get().creationTime) >= millisInterval
  }

  final def currentFileID: Long = currentCommitLogFileToPut.get().id

  private class CommitLogFile(val id: Long) {
    private[CommitLog] val absolutePath: String =
      new StringBuilder(pathWithSeparator)
        .append(id)
        .append(FilePathManager.DATAEXTENSION)
        .toString

    private val recordIDGen =
      new AtomicLong(0L)

    private val md5: MessageDigest =
      MessageDigest.getInstance("MD5")
    private val fileStream =
      new FileOutputStream(absolutePath)
    private val outputStream =
      new BufferedOutputStream(fileStream)
    private val digestOutputStream =
      new DigestOutputStream(outputStream, md5)
    private[CommitLog] val creationTime: Long =
      System.currentTimeMillis()

    private[CommitLog] def put(messageType: Byte, message: Array[Byte]): Unit = {
      val commitLogRecord =
        CommitLogRecord(
          messageType, message,
          System.currentTimeMillis()
        )

      val recordToBinary =
        commitLogRecord.toByteArray

      digestOutputStream.write(recordToBinary)
    }

    private[CommitLog] def flush(): Unit = {
      digestOutputStream.flush()
      outputStream.flush()
      fileStream.flush()
    }

    private[CommitLog] def close(withMD5: Boolean = true): Unit = {
      digestOutputStream.on(false)
      digestOutputStream.close()
      outputStream.close()
      fileStream.close()
      if (withMD5) {
        writeMD5File()
      }
    }

    private def writeMD5File() = {
      val fileMD5 = DatatypeConverter
        .printHexBinary(md5.digest())
        .getBytes

      val fileName = new StringBuilder(pathWithSeparator)
        .append(id).append(FilePathManager.MD5EXTENSION).toString

      new FileOutputStream(fileName) {
        write(fileMD5)
        close()
      }
    }
  }
}