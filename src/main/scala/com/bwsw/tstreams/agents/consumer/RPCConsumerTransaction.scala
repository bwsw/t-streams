package com.bwsw.tstreams.agents.consumer

/**
  * Created by ivan on 19.02.17.
  */
class RPCConsumerTransaction(s: String, n: String, p: Int, id: Long) extends transactionService.rpc.ConsumerTransaction {
  override def stream: String = s
  override def name: String = n
  override def partition: Int = p
  override def transactionID: Long = id
}
