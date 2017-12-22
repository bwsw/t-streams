package com.bwsw.tstreamstransactionserver.netty.server.db


abstract class KeyValueDbBatch() {
  def put(index: Int, key: Array[Byte], data: Array[Byte]): Boolean

  def remove(index: Int, key: Array[Byte]): Unit

  def write(): Boolean
}
