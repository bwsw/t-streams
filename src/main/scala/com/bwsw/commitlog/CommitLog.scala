package com.bwsw.commitlog

import java.io._
import java.security.{DigestOutputStream, MessageDigest}
import java.util.concurrent.atomic.AtomicLong
import javax.xml.bind.DatatypeConverter

import com.bwsw.commitlog.CommitLogFlushPolicy.{ICommitLogFlushPolicy, OnCountInterval, OnRotation, OnTimeInterval}
import com.bwsw.commitlog.filesystem.FilePathManager

import scala.annotation.tailrec

/** Logger which stores records continuously in files in specified location.
  *
  * Stores data in files placed YYYY/mm/dd/{serial number}.dat. If it works correctly, md5-files placed
  * YYYY/mm/dd/{serial number}.md5 shall be generated as well. New file starts on user request or when configured time
  * was exceeded.
  *
  * @param seconds period of time to write records into the same file, then start new file
  * @param path location to store files at
  * @param policy policy to flush data into file (OnRotation by default)
  */
class CommitLog(seconds: Int, path: String, policy: ICommitLogFlushPolicy = OnRotation, nextFileID: => Long) {
  require(seconds > 0, "Seconds cannot be less than 1")

  private val secondsInterval: Int = seconds

  @volatile private var fileCreationTime: Long = -1
  @volatile private var chunkWriteCount: Int = 0
  @volatile private var chunkOpenTime: Long = 0

  private var currentCommitLogFileToPut: CommitLogFile = _
  private class CommitLogFile(path: String, val id: Long) {
    val absolutePath: String = new StringBuffer(path).append(id).append(FilePathManager.DATAEXTENSION).toString
    private val recordIDGen = new AtomicLong(0L)

    private val md5: MessageDigest = MessageDigest.getInstance("MD5")
    private def writeMD5File() = {
      val fileMD5 = DatatypeConverter.printHexBinary(md5.digest()).getBytes
      new FileOutputStream(new StringBuffer(path).append(id).append(FilePathManager.MD5EXTENSION).toString) {
        write(fileMD5)
        close()
      }
    }

    private val outputStream = new BufferedOutputStream(new FileOutputStream(absolutePath, true))
    private val digestOutputStream = new DigestOutputStream(outputStream, md5)
    def put(messageType: Byte, message: Array[Byte]): Unit = {
      val commitLogRecord = CommitLogRecord(recordIDGen.getAndIncrement(), messageType, message)
      val recordToBinary = commitLogRecord.toByteArrayWithDelimiter
      digestOutputStream.write(recordToBinary)
    }

    def flush(): Unit = {
      digestOutputStream.flush()
      outputStream.flush()
    }

    def close(withMD5: Boolean = true): Unit = this.synchronized{
      digestOutputStream.on(false)
      digestOutputStream.flush()
      outputStream.flush()
      digestOutputStream.close()
      outputStream.close()
      if (withMD5) {
        writeMD5File()
      }
    }
  }

  private val pathWithSeparator = s"$path${java.io.File.separatorChar}"

  /** Puts record and its type to an appropriate file.
    *
    * Writes data to file in format (delimiter)(BASE64-encoded type and message). When writing to one file finished,
    * md5-sum file generated.
    *
    * @param message message to store
    * @param messageType type of message to store
    * @param startNew start new file if true
    * @return name of file record was saved in
    */
  def putRec(message: Array[Byte], messageType: Byte, startNew: Boolean = false): String = this.synchronized{

    // если хотим записать в новый файл при уже существующем коммит логе
    if (startNew && !firstRun) {
      resetCounters()
      currentCommitLogFileToPut.close()
      currentCommitLogFileToPut = new CommitLogFile(pathWithSeparator, nextFileID)
    }

    // если истекло время или мы начинаем записывать в новый коммит лог файл
    if (firstRun() || timeExceeded()) {
      if (!firstRun()) {
        resetCounters()
        currentCommitLogFileToPut.close()
      }
      fileCreationTime = getCurrentSecs()
      // TODO(remove this):write here chunkOpenTime or we will write first record instantly if OnTimeInterval policy set
      //      chunkOpenTime = System.currentTimeMillis()
      currentCommitLogFileToPut = new CommitLogFile(pathWithSeparator, nextFileID)
    }

    val now: Long = System.currentTimeMillis()
    policy match {
      case interval: OnTimeInterval if interval.seconds * 1000 + chunkOpenTime < now =>
        chunkOpenTime = now
        currentCommitLogFileToPut.flush()
      case interval: OnCountInterval if interval.count == chunkWriteCount =>
        chunkWriteCount = 0
        currentCommitLogFileToPut.flush()
      case _ =>
    }

    currentCommitLogFileToPut.put(messageType, message)

    chunkWriteCount += 1

    currentCommitLogFileToPut.absolutePath
  }

  /** Finishes work with current file. */
  def close(withMD5: Boolean = true): Option[String] = this.synchronized {
    if (!firstRun && currentCommitLogFileToPut != null) {
      resetCounters()
      currentCommitLogFileToPut.close(withMD5)
      Some(currentCommitLogFileToPut.absolutePath)
    } else None
  }

  final def currentFileID: Option[Long] = {
    if (currentCommitLogFileToPut != null)
      Some(currentCommitLogFileToPut.id)
    else
      None
  }

  private def resetCounters(): Unit = {
    fileCreationTime = -1
    chunkWriteCount = 0
    chunkOpenTime = System.currentTimeMillis()
  }

  private def firstRun() = {
    fileCreationTime == -1
  }

  private def getCurrentSecs(): Long = {
    System.currentTimeMillis() / 1000
  }

  private def timeExceeded(): Boolean = {
    getCurrentSecs - fileCreationTime >= secondsInterval
  }
}