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

abstract class CommitLogStorage
  extends Ordered[CommitLogStorage] {
  val id: Long

  val content: Array[Byte]

  final def compare(that: CommitLogStorage): Int =
    java.lang.Long.compare(id, that.id)

  /** Checks md5 sum of file with existing md5 sum. Throws an exception when no MD5 exists. */
  final def checkMD5(): Boolean = getMD5 sameElements calculateMD5

  /** Returns an iterator over records */
  def getIterator: CommitLogIterator

  /** Returns calculated MD5 of this file. */
  val calculateMD5: Array[Byte]

  /** Returns existing MD5 of this file. Throws an exception otherwise. */
  def getMD5: Array[Byte]

  /** Returns true if md5-sum exists. */
  def md5Exists(): Boolean

  /** D */
  def delete(): Unit
}
