package com.bwsw.tstreams.agents.producer

import com.bwsw.tstreams.agents.group.ProducerCheckpointInfo

/**
  * Created by Ivan Kudryavtsev on 29.08.16.
  */
trait IProducerTransaction {

  def send(obj: Array[Byte]): Unit

  def finalizeDataSend(): Unit

  def cancel(): Unit

  def checkpoint(isSynchronous: Boolean = true): Unit

  def getTransactionInfo(): ProducerCheckpointInfo

  def updateTransactionKeepAliveState(): Unit

  def isClosed(): Boolean

  def getTransactionID(): Long

  def markAsClosed(): Unit
}
