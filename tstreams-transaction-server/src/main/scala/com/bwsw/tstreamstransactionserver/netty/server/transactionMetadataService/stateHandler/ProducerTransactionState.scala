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

package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler

import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.{ProducerTransactionKey, ProducerTransactionRecord, ProducerTransactionValue}
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates.{Invalid, Opened}

abstract sealed class ProducerTransactionState(val producerTransactionRecord: ProducerTransactionRecord) {
  def handle(producerTransactionState: ProducerTransactionState): ProducerTransactionState
}


class OpenedTransactionState(producerTransactionRecord: ProducerTransactionRecord)
  extends ProducerTransactionState(producerTransactionRecord) {

  override def handle(producerTransactionState: ProducerTransactionState): ProducerTransactionState =
    producerTransactionState match {
      case _: OpenedTransactionState =>
        this

      case that: UpdatedTransactionState =>
        val thatTxn = that.producerTransactionRecord

        val transitedProducerTransaction =
          if (isThisProducerTransactionExpired(thatTxn))
            transitProducerTransactionToInvalidState()
          else {
            ProducerTransactionRecord(
              ProducerTransactionKey(thatTxn.stream, thatTxn.partition, thatTxn.transactionID),
              ProducerTransactionValue(Opened, thatTxn.quantity, thatTxn.ttl, thatTxn.timestamp)
            )
          }
        ProducerTransactionStateMachine(transitedProducerTransaction)

      case _: CanceledTransactionState =>
        ProducerTransactionStateMachine(transitProducerTransactionToInvalidState())

      case that: CheckpointedTransactionState =>
        val thatTxn = that.producerTransactionRecord

        if (isThisProducerTransactionExpired(thatTxn))
          ProducerTransactionStateMachine(transitProducerTransactionToInvalidState())
        else {
          that
        }

      case that =>
        new UndefinedTransactionState(that.producerTransactionRecord)
    }

  private def transitProducerTransactionToInvalidState() = {
    val txn = producerTransactionRecord
    ProducerTransactionRecord(
      ProducerTransactionKey(txn.stream, txn.partition, txn.transactionID),
      ProducerTransactionValue(Invalid, 0, 0L, txn.timestamp)
    )
  }

  private def isThisProducerTransactionExpired(that: ProducerTransactionRecord): Boolean = {
    val expirationPoint = producerTransactionRecord.timestamp + producerTransactionRecord.ttl
    scala.math.abs(expirationPoint) <= that.timestamp
  }
}

class UpdatedTransactionState(producerTransactionRecord: ProducerTransactionRecord)
  extends ProducerTransactionState(producerTransactionRecord) {
  override def handle(producerTransactionState: ProducerTransactionState): ProducerTransactionState =
    producerTransactionState match {
      case that =>
        new UndefinedTransactionState(that.producerTransactionRecord)
    }
}

class CanceledTransactionState(producerTransactionRecord: ProducerTransactionRecord)
  extends ProducerTransactionState(producerTransactionRecord) {
  override def handle(producerTransactionState: ProducerTransactionState): ProducerTransactionState =
    producerTransactionState match {
      case that =>
        new UndefinedTransactionState(that.producerTransactionRecord)
    }
}

class InvalidTransactionState(producerTransactionRecord: ProducerTransactionRecord)
  extends ProducerTransactionState(producerTransactionRecord) {
  override def handle(producerTransactionState: ProducerTransactionState): ProducerTransactionState =
    this
}

class CheckpointedTransactionState(producerTransactionRecord: ProducerTransactionRecord)
  extends ProducerTransactionState(producerTransactionRecord) {
  override def handle(producerTransactionState: ProducerTransactionState): ProducerTransactionState = {
    this
  }
}

class UndefinedTransactionState(producerTransactionRecord: ProducerTransactionRecord)
  extends ProducerTransactionState(producerTransactionRecord) {
  override def handle(producerTransactionState: ProducerTransactionState): ProducerTransactionState =
    producerTransactionState match {
      case that =>
        new UndefinedTransactionState(that.producerTransactionRecord)
    }
}

