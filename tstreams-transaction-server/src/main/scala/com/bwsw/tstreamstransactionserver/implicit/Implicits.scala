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
package com.bwsw.tstreamstransactionserver.`implicit`

import java.nio.ByteBuffer
import java.util.Comparator

import com.google.common.primitives.UnsignedBytes

import scala.language.implicitConversions

object Implicits {
  implicit def strToByteArray(str: String): Array[Byte] = str.getBytes

  implicit def intToByteArray(int: Int): Array[Byte] = ByteBuffer.allocate(java.lang.Integer.BYTES).putInt(int).array()

  implicit def longToByteArray(long: Long): Array[Byte] = ByteBuffer.allocate(java.lang.Long.BYTES).putLong(long).array()

  implicit def arrayByteTByteBuffer(array: Array[Byte]): java.nio.ByteBuffer = java.nio.ByteBuffer.wrap(array)

  implicit def arrayByteTByteBuffer(array: Seq[Array[Byte]]): Seq[java.nio.ByteBuffer] = array map arrayByteTByteBuffer

  implicit def byteBuffersToSeqArrayByte(buffers: Seq[java.nio.ByteBuffer]): Seq[Array[Byte]] = buffers map byteBufferToArrayByte

  implicit def byteBufferToArrayByte(buffer: java.nio.ByteBuffer): Array[Byte] = {
    val sizeOfSlicedData = buffer.limit() - buffer.position()
    val bytes = new Array[Byte](sizeOfSlicedData)
    buffer.get(bytes)
    bytes
  }

  val ByteArray: Comparator[Array[Byte]] = UnsignedBytes.lexicographicalComparator()
}
