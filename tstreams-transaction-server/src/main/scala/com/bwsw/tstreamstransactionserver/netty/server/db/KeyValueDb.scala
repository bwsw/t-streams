package com.bwsw.tstreamstransactionserver.netty.server.db

abstract class KeyValueDb {
  def get(key: Array[Byte]): Array[Byte]

  def put(key: Array[Byte], data: Array[Byte]): Boolean

  def delete(key: Array[Byte]): Boolean

  def iterator: KeyValueDbIterator
}
