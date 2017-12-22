package com.bwsw.tstreamstransactionserver.netty.client.api

import scala.concurrent.{Future => ScalaFuture}

trait ConsumerClientApi {
  def putConsumerCheckpoint(consumerTransaction: com.bwsw.tstreamstransactionserver.rpc.ConsumerTransaction): ScalaFuture[Boolean]

  def getConsumerState(name: String, streamID: Int, partition: Int): ScalaFuture[Long]
}
