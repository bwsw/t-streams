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

import java.io.ByteArrayInputStream
import java.security.MessageDigest
import javax.xml.bind.DatatypeConverter

class CommitLogBinary(override val id: Long,
                      override val content: Array[Byte],
                      md5: Option[Array[Byte]])
  extends CommitLogStorage {

  require(md5.exists(_.length == 32))

  /** bytes to read from this file */
  private val chunkSize = 100000


  override final lazy val calculateMD5: Array[Byte] = {
    val stream =
      new ByteArrayInputStream(content)

    val md5: MessageDigest =
      MessageDigest.getInstance("MD5")
    md5.reset()

    val chunk = new Array[Byte](chunkSize)
    while (stream.available() > 0) {
      val bytesRead = stream.read(chunk)
      md5.update(chunk.take(bytesRead))
    }

    stream.close()

    DatatypeConverter
      .printHexBinary(md5.digest())
      .getBytes
  }


  /** Returns an iterator over records */
  override final def getIterator: CommitLogIterator =
    new CommitLogBinaryIterator(new ByteArrayInputStream(content))

  override final def getMD5: Array[Byte] =
    md5.getOrElse(throw new IllegalArgumentException("There is no md5 sum!"))

  override final def md5Exists(): Boolean =
    md5.isDefined

  override final def delete(): Unit = {}

  override final def toString: String = {
    s"Commit log binary[id: $id]"
  }
}
