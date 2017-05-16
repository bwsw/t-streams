package com.bwsw.tstreams.agents.consumer


/**
  * Created by ivan on 19.02.17.
  */
class RPCConsumerTransaction(consumerName: String, streamID: Int, partitionNo: Int, transaction: Long)
  extends com.bwsw.tstreamstransactionserver.rpc.ConsumerTransaction {

  override def stream: Int = streamID

  override def name: String = consumerName

  override def partition: Int = partitionNo

  override def transactionID: Long = transaction
}
