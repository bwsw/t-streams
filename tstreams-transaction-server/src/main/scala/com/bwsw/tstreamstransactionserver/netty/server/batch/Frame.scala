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

package com.bwsw.tstreamstransactionserver.netty.server.batch

import java.nio.ByteBuffer

import com.bwsw.tstreamstransactionserver.netty.Protocol
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.Structure
import com.bwsw.tstreamstransactionserver.rpc.ProducerTransactionsAndData
import com.bwsw.tstreamstransactionserver.rpc.TransactionService._

object Frame {

  val Timestamp: Byte = 0
  val PutTransactionDataType: Byte = 1
  val PutSimpleTransactionAndDataType: Byte = 2
  val PutProducerStateWithDataType: Byte = 3
  val PutTransactionType: Byte = 4
  val PutTransactionsType: Byte = 5
  val TokenCreatedType: Byte = 7
  val TokenUpdatedType: Byte = 8
  val TokenExpiredType: Byte = 9


  def deserializePutTransactionData(message: Array[Byte]): PutTransactionData.Args =
    Protocol.PutTransactionData.decodeRequest(message)

  def deserializePutTransaction(message: Array[Byte]): PutTransaction.Args =
    Protocol.PutTransaction.decodeRequest(message)

  def deserializePutTransactions(message: Array[Byte]): PutTransactions.Args =
    Protocol.PutTransactions.decodeRequest(message)

  def deserializePutSimpleTransactionAndData(message: Array[Byte]): ProducerTransactionsAndData =
    Structure.PutTransactionsAndData.decode(message)

  def deserializePutProducerStateWithData(message: Array[Byte]): PutProducerStateWithData.Args =
    Protocol.PutProducerStateWithData.decodeRequest(message)

  def deserializeToken(bytes: Array[Byte]): Int = ByteBuffer.wrap(bytes).getInt
}

class Frame(val typeId: Byte,
            val timestamp: Long,
            val token: Int,
            val body: Array[Byte])
  extends Ordered[Frame] {

  override def equals(obj: scala.Any): Boolean = obj match {
    case that: Frame =>
      typeId == that.typeId &&
        timestamp == that.timestamp &&
        token == that.token &&
        body.sameElements(that.body)
    case _ =>
      false
  }

  override def hashCode(): Int = {
    31 * (
      31 * (
        31 * (
          31 + token.hashCode()
          ) + timestamp.hashCode()
        ) + typeId.hashCode()
      ) + java.util.Arrays.hashCode(body)
  }

  override def compare(that: Frame): Int = {
    if (this.timestamp < that.timestamp) -1
    else if (this.timestamp > that.timestamp) 1
    else if (this.typeId < that.typeId) -1
    else if (this.typeId > that.typeId) 1
    else 0
  }
}
