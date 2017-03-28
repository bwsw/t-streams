package com.bwsw.tstreams.agents.producer

import com.bwsw.tstreamstransactionserver.rpc.TransactionStates

/**
  * Created by Ivan Kudryavtsev on 28.03.17.
  */
class RPCProducerTransaction(s: String, part: Int, tid: Long, st: TransactionStates, qty: Int, t: Long) extends com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction {
  override def stream: String = s

  override def transactionID: Long = tid

  override def state: TransactionStates = st

  override def quantity: Int = qty

  override def partition: Int = part

  override def ttl: Long = t
}
