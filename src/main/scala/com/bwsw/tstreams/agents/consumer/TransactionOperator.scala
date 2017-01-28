package com.bwsw.tstreams.agents.consumer

import scala.collection.mutable.ListBuffer

/**
  * Created by Ivan Kudryavtsev on 22.08.16.
  * Abstract type for Consumer
  */
trait TransactionOperator[T] {
  def getLastTransaction(partition: Int): Option[ConsumerTransaction[T]]

  def getTransactionById(partition: Int, transactionID: Long): Option[ConsumerTransaction[T]]

  def buildTransactionObject(partition: Int, transactionID: Long, count: Int): Option[ConsumerTransaction[T]]

  def setStreamPartitionOffset(partition: Int, transactionID: Long): Unit

  def loadTransactionFromDB(partition: Int, transactionID: Long): Option[ConsumerTransaction[T]]

  def getTransactionsFromTo(partition: Int, from: Long, to: Long): ListBuffer[ConsumerTransaction[T]]

  def checkpoint(): Unit

  def getPartitions(): Set[Int]

  def getCurrentOffset(partition: Int): Long

  def getProposedTransactionId(): Long
}