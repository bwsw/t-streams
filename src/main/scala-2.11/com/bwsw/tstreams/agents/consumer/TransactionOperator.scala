package com.bwsw.tstreams.agents.consumer

import java.util.UUID

import scala.collection.mutable.ListBuffer

/**
  * Created by Ivan Kudryavtsev on 22.08.16.
  * Abstract type for Consumer
  */
trait TransactionOperator[T] {
  def getLastTransaction(partition: Int): Option[ConsumerTransaction[T]]

  def getTransactionsFromTo(partition: Int, from: UUID, to: UUID): ListBuffer[ConsumerTransaction[T]]

  def getTransactionById(partition: Int, transactionUUID: UUID): Option[ConsumerTransaction[T]]

  def buildTransactionObject(partition: Int, transactionUUID: UUID, count: Int): Option[ConsumerTransaction[T]]

  def setStreamPartitionOffset(partition: Int, uuid: UUID): Unit

  def loadTransactionFromDB(partition: Int, transactionUUID: UUID): Option[ConsumerTransaction[T]]

  def checkpoint(): Unit

  def getPartitions(): Set[Int]

  def getCurrentOffset(partition: Int): UUID
}
