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

class FileValue(val fileContent: Array[Byte], val fileMD5Content: Option[Array[Byte]]) {
  final def toByteArray: Array[Byte] = fileMD5Content
    .map{md5Content =>
      require(md5Content.length == FileValue.MD5_SUM_LENGTH)
      (FileValue.MD5_FILE_EXIST +: md5Content) ++ fileContent
    }
    .getOrElse(FileValue.NO_MD5_FILE +: fileContent)


  override def hashCode(): Int = {
    val prime = 31

    val md5HashCode = fileMD5Content
      .map(java.util.Arrays.hashCode)
      .getOrElse(1)

    prime * (java.util.Arrays.hashCode(fileContent) + md5HashCode)
  }

  override def equals(that: scala.Any): Boolean = that match {
    case fileValue: FileValue =>
      (fileContent sameElements fileValue.fileContent) && ((fileMD5Content, fileValue.fileMD5Content) match {
        case (Some(thisMD5), Some(thatMD5)) => thisMD5 sameElements thatMD5
        case (None, None) => true
        case _ => false
      })
    case _ => false
  }
}

object FileValue {
  private val NO_MD5_FILE = 1: Byte
  private val MD5_FILE_EXIST = 2: Byte
  private val MD5_SUM_LENGTH = 32

  def apply(fileContent: Array[Byte], fileMD5Content: Option[Array[Byte]]): FileValue = new FileValue(fileContent, fileMD5Content)

  final def fromByteArray(bytes: Array[Byte]): FileValue = {
    val md5_flag = bytes.head
    val other = bytes.tail
    if (md5_flag == NO_MD5_FILE)
      new FileValue(other, None)
    else {
      val (md5Sum, fileContent) = other.splitAt(MD5_SUM_LENGTH)
      new FileValue(fileContent, Some(md5Sum))
    }
  }
}
