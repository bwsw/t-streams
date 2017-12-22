package com.bwsw.tstreamstransactionserver.netty.client.api

import scala.concurrent.{Future => ScalaFuture}

trait StreamClientApi {
  def putStream(stream: String, partitions: Int, description: Option[String], ttl: Long): ScalaFuture[Int]

  def putStream(stream: com.bwsw.tstreamstransactionserver.rpc.StreamValue): ScalaFuture[Int]

  def delStream(name: String): ScalaFuture[Boolean]

  def getStream(name: String): ScalaFuture[Option[com.bwsw.tstreamstransactionserver.rpc.Stream]]

  def checkStreamExists(name: String): ScalaFuture[Boolean]
}
