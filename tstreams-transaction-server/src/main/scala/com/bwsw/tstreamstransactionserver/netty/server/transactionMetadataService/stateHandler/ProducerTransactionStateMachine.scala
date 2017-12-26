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

import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.ProducerTransactionRecord
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates._

import scala.annotation.tailrec


object ProducerTransactionStateMachine {
  def checkFinalStateBeStoredInDB(producerTransactionState: ProducerTransactionState): Boolean = {
    producerTransactionState match {
      case _: OpenedTransactionState =>
        true
      case _: InvalidTransactionState =>
        true
      case _: CheckpointedTransactionState =>
        true
      case _ =>
        false
    }
  }

  final def transiteTransactionsToFinalState(records: Seq[ProducerTransactionRecord]): Option[ProducerTransactionState] = {
    transiteTransactionsToFinalState(records, _ => {})
  }

  final def transiteTransactionsToFinalState(records: Seq[ProducerTransactionRecord],
                                             onTransitionDo: ProducerTransactionRecord => Unit): Option[ProducerTransactionState] = {
    @tailrec
    def go(records: List[ProducerTransactionRecord],
           currentState: ProducerTransactionState): ProducerTransactionState = {
      records match {
        case Nil =>
          onTransitionDo(currentState.producerTransactionRecord)
          currentState
        case head :: tail =>
          val newState = currentState.handle(
            ProducerTransactionStateMachine(head)
          )
          onTransitionDo(newState.producerTransactionRecord)
          if (newState == currentState ||
            newState.isInstanceOf[UndefinedTransactionState]) {
            currentState
          }
          else {
            go(tail, newState)
          }
      }
    }

    val recordsLst =
      records.toList
    recordsLst
      .headOption
      .map(rootRecord =>
        go(recordsLst.tail, ProducerTransactionStateMachine(rootRecord))
      )
  }

  def apply(producerTransactionRecord: ProducerTransactionRecord): ProducerTransactionState = {
    producerTransactionRecord.state match {
      case Opened =>
        new OpenedTransactionState(producerTransactionRecord)
      case Updated =>
        new UpdatedTransactionState(producerTransactionRecord)
      case Cancel =>
        new CanceledTransactionState(producerTransactionRecord)
      case Invalid =>
        new InvalidTransactionState(producerTransactionRecord)
      case Checkpointed =>
        new CheckpointedTransactionState(producerTransactionRecord)
      case _ =>
        new UndefinedTransactionState(producerTransactionRecord)
    }
  }
}
