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

import java.io._
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.{Files, Paths}
import java.security.MessageDigest
import javax.xml.bind.DatatypeConverter

import org.apache.commons.io.IOUtils

/** Represents commitlog file with data.
  *
  * @param path full path to file
  */
class CommitLogFile(path: String)
  extends CommitLogStorage {
  //todo CommitLogFile существует в двух реализациях: private класс(внутри CommitLog) и этот класс. Требуется рефакторинг (может достаточно переименования)
  private val file = {
    val file = new File(path)
    if (file.exists())
      file
    else
      throw new IOException(s"File ${file.getPath} doesn't exist!")
  }

  private val md5File =
    new File(file.toString.split('.')(0) + FilePathManager.MD5EXTENSION)

  /** bytes to read from this file */
  private val chunkSize = 100000


  override final val id: Long = file
    .getName.dropRight(FilePathManager.DATAEXTENSION.length)
    .toLong

  override final val content: Array[Byte] = {
    val fileInputStream = new FileInputStream(file)
    val content = IOUtils.toByteArray(fileInputStream)
    fileInputStream.close()
    content
  }

  override final lazy val calculateMD5: Array[Byte] = {
    val fileInputStream =
      new FileInputStream(file)
    val stream =
      new BufferedInputStream(fileInputStream)

    val md5: MessageDigest =
      MessageDigest.getInstance("MD5")
    md5.reset()

    val chunk = new Array[Byte](chunkSize)
    while (stream.available() > 0) {
      val bytesRead = stream.read(chunk)
      md5.update(chunk.take(bytesRead))
    }

    stream.close()
    fileInputStream.close()

    DatatypeConverter
      .printHexBinary(md5.digest())
      .getBytes
  }

  /** Returns underlying file. */
  def getFile: File = file


  override final def getIterator: CommitLogIterator =
    new CommitLogFileIterator(file.toString)

  override final def getMD5: Array[Byte] =
    if (!md5Exists())
      throw new FileNotFoundException("No MD5 file for " + path)
    else
      getContentOfMD5File


  private def getContentOfMD5File = {
    val fileInputStream = new FileInputStream(md5File)
    val md5Sum = new Array[Byte](32)
    fileInputStream.read(md5Sum)
    fileInputStream.close()
    md5Sum
  }

  /** Delete file */
  override final def delete(): Unit = {
    file.delete() && (
      if (md5Exists())
        md5File.delete()
      else
        true
      )
  }

  /** Returns true if md5-file exists. */
  override final def md5Exists(): Boolean =
    md5File.exists()

  override final def toString: String = {
    s"Commit log file[id: $id, path: ${file.getPath}]"
  }
}
