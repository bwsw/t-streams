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
package com.bwsw.tstreamstransactionserver.netty.server.commitLogService

import java.util.concurrent.PriorityBlockingQueue

import com.bwsw.commitlog.CommitLogFlushPolicy.{OnCountInterval, OnRotation, OnTimeInterval}
import com.bwsw.commitlog.filesystem.{CommitLogFile, CommitLogStorage}
import com.bwsw.commitlog.{CommitLog, IDGenerator}
import com.bwsw.tstreamstransactionserver.options.CommitLogWriteSyncPolicy.{EveryNSeconds, EveryNewFile, EveryNth}
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.{CommitLogOptions, StorageOptions}
import org.slf4j.LoggerFactory

class ScheduledCommitLog(pathsToClosedCommitLogFiles: PriorityBlockingQueue[CommitLogStorage],
                         storageOptions: StorageOptions,
                         commitLogOptions: CommitLogOptions,
                         iDGenerator: IDGenerator[Long])
  extends Runnable {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val commitLog = createCommitLog()

  def currentCommitLogFile: Long = commitLog.currentFileID

  def putData(messageType: Byte, message: Array[Byte]) = {
    commitLog.putRec(message, messageType)
    true
  }

  def closeWithoutCreationNewFile() = {
    val path = commitLog.close(createNewFile = false)
    pathsToClosedCommitLogFiles.put(new CommitLogFile(path))
  }

  override def run(): Unit = {
    val path = commitLog.close()
    pathsToClosedCommitLogFiles.put(new CommitLogFile(path))
  }

  private def createCommitLog() = {
    val policy = commitLogOptions.syncPolicy match {
      case EveryNth => OnCountInterval(commitLogOptions.syncValue)
      case EveryNewFile => OnRotation
      case EveryNSeconds => OnTimeInterval(commitLogOptions.syncValue)
    }
    new CommitLog(Int.MaxValue,
      s"${storageOptions.path}${java.io.File.separatorChar}${storageOptions.commitLogRawDirectory}",
      policy,
      iDGenerator
    )
  }
}