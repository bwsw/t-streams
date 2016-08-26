package com.bwsw.tstreams.agents.consumer

import java.util.UUID

import scala.collection.mutable.ListBuffer

/**
  * Created by Ivan Kudryavtsev on 22.08.16.
  * Abstract type for Consumer
  */
trait TransactionOperator[T] {
  def getLastTransaction(partition: Int): Option[Transaction[T]]
  def getTransactionsFromTo(partition: Int, from: UUID, to: UUID): ListBuffer[Transaction[T]]
  def getTransactionById(partition: Int, uuid: UUID): Option[Transaction[T]]
  def setStreamPartitionOffset(partition: Int, uuid: UUID): Unit
  def updateTransactionInfoFromDB(txn: UUID, partition: Int): Option[Transaction[T]]
  def checkpoint(): Unit
  def getPartitions(): Set[Int]
  def getCurrentOffset(partition: Int): UUID
}
