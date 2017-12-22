package com.bwsw.tstreamstransactionserver.netty.server.db

abstract class KeyValueDbManager {
  def getDatabase(index: Int): KeyValueDb

  def newBatch: KeyValueDbBatch

  def closeDatabases(): Unit
}
