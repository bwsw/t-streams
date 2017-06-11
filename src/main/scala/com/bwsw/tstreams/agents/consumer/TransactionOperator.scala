package com.bwsw.tstreams.agents.consumer

import com.bwsw.tstreamstransactionserver.rpc.TransactionStates

import scala.collection.mutable.ListBuffer

/**
  * Created by Ivan Kudryavtsev on 22.08.16.
  * Abstract type for Consumer
  */
private[tstreams] trait TransactionOperator {
  def getLastTransaction(partition: Int): Option[ConsumerTransaction]

  def getTransactionById(partition: Int, transactionID: Long): Option[ConsumerTransaction]

  def buildTransactionObject(partition: Int, transactionID: Long, state: TransactionStates, count: Int): Option[ConsumerTransaction]

  def setStreamPartitionOffset(partition: Int, transactionID: Long): Unit

  def loadTransactionFromDB(partition: Int, transactionID: Long): Option[ConsumerTransaction]

  def getTransactionsFromTo(partition: Int, from: Long, to: Long): ListBuffer[ConsumerTransaction]

  def checkpoint(): Unit

  def getPartitions: Set[Int]

  def getCurrentOffset(partition: Int): Long

  def getProposedTransactionId: Long
}
