package com.bwsw.tstreamstransactionserver.netty.client.api

import scala.concurrent.{Future => ScalaFuture}

trait DataClientApi {
  def putTransactionData(streamID: Int, partition: Int, transaction: Long, data: Seq[Array[Byte]], from: Int): ScalaFuture[Boolean]

  def getTransactionData(streamID: Int, partition: Int, transaction: Long, from: Int, to: Int): ScalaFuture[Seq[Array[Byte]]]
}
