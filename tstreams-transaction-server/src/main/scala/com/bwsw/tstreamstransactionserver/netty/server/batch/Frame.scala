package com.bwsw.tstreamstransactionserver.netty.server.batch

import com.bwsw.tstreamstransactionserver.netty.Protocol
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.Structure
import com.bwsw.tstreamstransactionserver.rpc.ProducerTransactionsAndData
import com.bwsw.tstreamstransactionserver.rpc.TransactionService._

object Frame
  extends Enumeration {

  val Timestamp = Value(0)
  val PutTransactionDataType = Value(1)
  val PutSimpleTransactionAndDataType = Value(2)
  val PutProducerStateWithDataType = Value(3)
  val PutTransactionType = Value(4)
  val PutTransactionsType = Value(5)
  val PutConsumerCheckpointType = Value(6)


  def deserializePutTransactionData(message: Array[Byte]): PutTransactionData.Args =
    Protocol.PutTransactionData.decodeRequest(message)

  def deserializePutTransaction(message: Array[Byte]): PutTransaction.Args =
    Protocol.PutTransaction.decodeRequest(message)

  def deserializePutTransactions(message: Array[Byte]): PutTransactions.Args =
    Protocol.PutTransactions.decodeRequest(message)

  def deserializePutSimpleTransactionAndData(message: Array[Byte]): ProducerTransactionsAndData =
    Structure.PutTransactionsAndData.decode(message)

  def deserializePutConsumerCheckpoint(message: Array[Byte]): PutConsumerCheckpoint.Args =
    Protocol.PutConsumerCheckpoint.decodeRequest(message)

  def deserializePutProducerStateWithData(message: Array[Byte]): PutProducerStateWithData.Args =
    Protocol.PutProducerStateWithData.decodeRequest(message)
}

abstract class Frame(val typeId: Byte,
                     val timestamp: Long,
                     val body: Array[Byte])
  extends Ordered[Frame] {

  override def equals(obj: scala.Any): Boolean = obj match {
    case that: Frame =>
      typeId == that.typeId &&
        timestamp == that.timestamp &&
        body.sameElements(that.body)
    case _ =>
      false
  }

  override def hashCode(): Int = {
    31 * (
      31 * (
        31 + timestamp.hashCode()
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
