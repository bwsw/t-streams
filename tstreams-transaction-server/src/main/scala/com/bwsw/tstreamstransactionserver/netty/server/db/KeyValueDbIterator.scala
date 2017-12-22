package com.bwsw.tstreamstransactionserver.netty.server.db

trait KeyValueDbIterator {
  def key(): Array[Byte]

  def value(): Array[Byte]

  def isValid: Boolean

  def seekToFirst(): Unit

  def seekToLast(): Unit

  def seek(target: Array[Byte]): Unit

  def next(): Unit

  def prev(): Unit

  def close(): Unit
}
