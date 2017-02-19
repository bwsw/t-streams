package com.bwsw.tstreams.agents.producer

import transactionService.rpc.TransactionStates

/**
  * Created by Ivan Kudryavtsev on 19.02.17.
  */
class RPCProducerTransaction(_stream: String, _partition: Int, _transactionID: Long, _ttl: Long, _state: TransactionStates, _quantity: Int) extends transactionService.rpc.ProducerTransaction {
  override def stream: String = this._stream

  override def partition: Int = _partition

  override def transactionID: Long = _transactionID

  override def keepAliveTTL: Long = _ttl

  override def state: TransactionStates = _state

  override def quantity: Int = _quantity
}
