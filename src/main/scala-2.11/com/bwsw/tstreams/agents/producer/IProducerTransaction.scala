package com.bwsw.tstreams.agents.producer

import java.util.UUID

import com.bwsw.tstreams.agents.group.ProducerCheckpointInfo

/**
  * Created by Ivan Kudryavtsev on 29.08.16.
  */
trait IProducerTransaction[T] {
  def awaitMaterialized(): Unit
  def send(obj: T): Unit
  def finalizeDataSend(): Unit
  def cancel(): Unit
  def checkpoint(isSynchronous: Boolean = true): Unit
  def getTransactionInfo(): ProducerCheckpointInfo
  def updateTxnKeepAliveState(): Unit
  def isClosed(): Boolean
  def getTransactionUUID(): UUID
  def makeMaterialized(): Unit
}
