package com.bwsw.tstreamstransactionserver.netty.client.api

import com.bwsw.tstreamstransactionserver.rpc.{ScanTransactionsInfo, TransactionInfo, TransactionStates}

import scala.concurrent.{Future => ScalaFuture}

trait MetadataClientApi {
  def getTransaction(): ScalaFuture[Long]

  def getTransaction(timestamp: Long): ScalaFuture[Long]

  def putTransactions(producerTransactions: Seq[com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction],
                      consumerTransactions: Seq[com.bwsw.tstreamstransactionserver.rpc.ConsumerTransaction]): ScalaFuture[Boolean]

  def putProducerState(transaction: com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction): ScalaFuture[Boolean]

  def putTransaction(transaction: com.bwsw.tstreamstransactionserver.rpc.ConsumerTransaction): ScalaFuture[Boolean]

  def openTransaction(streamID: Int, partitionID: Int, transactionTTLMs: Long): ScalaFuture[Long]

  def getTransaction(streamID: Int, partition: Int, transaction: Long): ScalaFuture[TransactionInfo]

  def getLastCheckpointedTransaction(streamID: Int, partition: Int): ScalaFuture[Long]

  def scanTransactions(streamID: Int, partition: Int, from: Long, to: Long, count: Int, states: Set[TransactionStates]): ScalaFuture[ScanTransactionsInfo]
}
