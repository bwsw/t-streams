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

package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy


object RootNodeData {
  val delimiterIndexFieldSize: Int = java.lang.Integer.BYTES

  def fromByteArray(bytes: Array[Byte]): RootNodeData = {

    if (bytes.isEmpty) {
      new RootNodeData(
        Array.emptyByteArray,
        Array.emptyByteArray
      )
    }
    else {
      val buf = java.nio.ByteBuffer
        .wrap(bytes)

      val firstSize = buf.getInt
      val first = new Array[Byte](firstSize)
      buf.get(first)

      val second = new Array[Byte](buf.remaining())
      buf.get(second)

      new RootNodeData(
        first,
        second
      )
    }
  }

  def apply(firstID: Array[Byte],
            lastID: Array[Byte]): RootNodeData =
    new RootNodeData(firstID, lastID)
}


class RootNodeData(val firstID: Array[Byte],
                   val lastID: Array[Byte]) {
  def toByteArray: Array[Byte] = {
    val size =
      RootNodeData.delimiterIndexFieldSize +
        firstID.length +
        lastID.length

    val buf = java.nio.ByteBuffer
      .allocate(size)
      .putInt(firstID.length)
      .put(firstID)
      .put(lastID)
    buf.flip()

    if (buf.hasArray) {
      buf.array()
    }
    else {
      val bytes = new Array[Byte](size)
      buf.get(bytes)
      bytes
    }
  }

  override def hashCode(): Int = {
    val firstIdHash =
      java.util.Arrays.hashCode(firstID)

    val lastIdHash =
      java.util.Arrays.hashCode(lastID)

    31 * (
      31 + firstIdHash.hashCode()
      ) + lastIdHash.hashCode()
  }

  override def equals(o: scala.Any): Boolean = o match {
    case that: RootNodeData =>
      this.firstID.sameElements(that.firstID) &&
        this.lastID.sameElements(that.lastID)
    case _ => false
  }
}
