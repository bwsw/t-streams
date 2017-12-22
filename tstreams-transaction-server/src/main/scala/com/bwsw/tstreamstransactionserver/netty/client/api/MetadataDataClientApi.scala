package com.bwsw.tstreamstransactionserver.netty.client.api

import scala.concurrent.{Future => ScalaFuture}

trait MetadataDataClientApi
  extends MetadataClientApi
    with DataClientApi {
  def putProducerStateWithData(producerTransaction: com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction, data: Seq[Array[Byte]], from: Int): ScalaFuture[Boolean]

  def putSimpleTransactionAndData(streamID: Int, partition: Int, data: Seq[Array[Byte]]): ScalaFuture[Long]

  def putSimpleTransactionAndDataWithoutResponse(streamID: Int, partition: Int, data: Seq[Array[Byte]])
}
