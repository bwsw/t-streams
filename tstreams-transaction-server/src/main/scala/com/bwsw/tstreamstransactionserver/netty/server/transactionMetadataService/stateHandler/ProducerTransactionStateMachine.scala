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
