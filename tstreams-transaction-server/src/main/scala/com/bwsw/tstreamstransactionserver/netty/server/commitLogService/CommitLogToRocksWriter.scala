
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

import java.util
import java.util.concurrent.BlockingQueue

import com.bwsw.commitlog.filesystem.{CommitLogIterator, CommitLogStorage}
import com.bwsw.tstreamstransactionserver.netty.server.batch.{BigCommit, BigCommitWithFrameParser}
import com.bwsw.tstreamstransactionserver.netty.server.db.rocks.RocksDbConnection
import com.bwsw.tstreamstransactionserver.netty.server.storage.Storage
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.CommitLogKey
import com.bwsw.tstreamstransactionserver.netty.server.RocksWriter
import com.bwsw.tstreamstransactionserver.options.IncompleteCommitLogReadPolicy.{Error, IncompleteCommitLogReadPolicy, SkipLog, TryRead}
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import CommitLogToRocksWriter.recordsToReadNumber
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.commitLogService.CommitLogService

private object CommitLogToRocksWriter {
  val recordsToReadNumber = 1
}

class CommitLogToRocksWriter(rocksDb: RocksDbConnection,
                             pathsToClosedCommitLogFiles: BlockingQueue[CommitLogStorage],
                             rocksWriter: RocksWriter,
                             commitLogService: CommitLogService,
                             incompleteCommitLogReadPolicy: IncompleteCommitLogReadPolicy)
  extends Runnable {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private def processAccordingToPolicy(commitLogEntity: CommitLogStorage) = {
    if (commitLogService.getLastProcessedCommitLogFileID < commitLogEntity.id) {
      val fileKey = FileKey(commitLogEntity.id)
      val fileValue = FileValue(commitLogEntity.content,
        if (commitLogEntity.md5Exists())
          Some(commitLogEntity.getMD5)
        else None
      )

      rocksDb.put(fileKey.toByteArray(), fileValue.toByteArray)
      incompleteCommitLogReadPolicy match {
        case SkipLog =>
          if (commitLogEntity.md5Exists()) {
            processCommitLogFile(commitLogEntity)
          }
          else {
            if (logger.isWarnEnabled()) {
              logger.warn(s"MD5 doesn't exist for $commitLogEntity")
            }
          }
          commitLogEntity.delete()

        case TryRead =>
          scala.util.Try {
            processCommitLogFile(commitLogEntity)
          } match {
            case scala.util.Failure(exception) =>
              if (logger.isWarnEnabled()) {
                logger.warn(s"Something was going wrong during processing of $commitLogEntity", exception.getMessage)
              }
            case _ =>
          }
          commitLogEntity.delete()


        case Error =>
          if (commitLogEntity.md5Exists()) {
            processCommitLogFile(commitLogEntity)
          }
          else {
            commitLogEntity.delete()
            throw new InterruptedException(s"MD5 doesn't exist in $commitLogEntity")
          }
      }
    }
  }


  private final def getRecordsReader(fileID: Long): BigCommitWithFrameParser = {
    val value =
      CommitLogKey(fileID).toByteArray
    val bigCommit =
      new BigCommit(rocksWriter, Storage.COMMIT_LOG_STORE, BigCommit.commitLogKey, value)
    new BigCommitWithFrameParser(bigCommit)
  }

  private def readRecordsFromCommitLogFile(iter: CommitLogIterator,
                                           recordsToReadNumber: Int): (ArrayBuffer[CommitLogRecordFrame], CommitLogIterator) = {
    val buffer = ArrayBuffer.empty[CommitLogRecordFrame]
    var recordsToRead = recordsToReadNumber
    while (iter.hasNext() && recordsToRead > 0) {
      val record = iter.next()
      record.right.foreach(buffer += new CommitLogRecordFrame(_))
      recordsToRead = recordsToRead - 1
    }
    (buffer, iter)
  }

  @tailrec
  private final def processOneRecord(iterator: CommitLogIterator,
                                     commitLogReader: BigCommitWithFrameParser,
                                     lastTransactionTimestamp: Option[Long]): Option[Long] = {

    val (commitLogRecords, iter) =
      readRecordsFromCommitLogFile(iterator, recordsToReadNumber)

    commitLogReader.addFrames(commitLogRecords)

    val isAnyElements = iter.hasNext()
    if (isAnyElements) {
      val lastTransactionTimestampInRecord =
        commitLogRecords
          .lastOption.map(_.timestamp)
          .orElse(lastTransactionTimestamp)

      processOneRecord(
        iter,
        commitLogReader,
        lastTransactionTimestampInRecord
      )
    }
    else
      lastTransactionTimestamp
  }

  @throws[Exception]
  private def processCommitLogFile(file: CommitLogStorage): Boolean = {
    val commitLogReader: BigCommitWithFrameParser =
      getRecordsReader(file.id)

    val iterator =
      file.getIterator
    val lastTransactionTimestamp =
      processOneRecord(iterator, commitLogReader, None)

    iterator.close()

    val result = commitLogReader.commit()

    val timestamp =
      lastTransactionTimestamp.getOrElse(System.currentTimeMillis())
    rocksWriter
      .createAndExecuteTransactionsToDeleteTask(timestamp)

    rocksWriter
      .clearProducerTransactionCache()
    result
  }


  override def run(): Unit = {
    val commitLogFiles = new util.LinkedList[CommitLogStorage]()
    pathsToClosedCommitLogFiles.drainTo(commitLogFiles)
    commitLogFiles.forEach(commitLogEntity =>
      scala.util.Try {
        processAccordingToPolicy(commitLogEntity)
      } match {
        case scala.util.Success(_) =>
        case scala.util.Failure(error) =>
          error match {
            case _: java.lang.InterruptedException =>
              Thread.currentThread().interrupt()
            case _ =>
              error.printStackTrace()
          }
      }
    )
  }
}