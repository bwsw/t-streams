package com.bwsw.tstreams.transactionServer.rpcImpl

import java.nio.ByteBuffer

import com.bwsw.tstreams.transactionServer.rpc.{Stream, Transaction, TransactionServerService, TransactionStates}
import com.twitter.util.Future

/**
  * Created by Ivan Kudryavtsev on 22.10.16.
  */
class TransactionServerServiceImpl extends TransactionServerService[Future] {
  override def authenticate(login: String, password: String): Future[String] = ???

  override def putTransaction(token: String, stream: String, partition: Int, interval: Long, transaction: Long, state: TransactionStates, quantity: Int, timestamp: Int): Future[Boolean] = ???

  override def scanTransactions(token: String, stream: String, partition: Int, interval: Long): Future[Seq[Transaction]] = ???

  override def scanTransactionsCRC32(token: String, stream: String, partition: Int, interval: Long): Future[Int] = ???

  override def delTransaction(token: String, stream: String, partition: Int, interval: Long, transaction: Long): Future[Boolean] = ???

  override def putStream(token: String, stream: String, partition: Int, partitions: Int, description: String): Future[Boolean] = ???

  override def delStream(token: String, stream: String): Future[Boolean] = ???

  override def getStream(token: String, stream: String): Future[Stream] = ???

  override def setConsumerState(token: String, name: String, stream: String, partition: Int, transaction: Long): Future[Boolean] = ???

  override def getConsumerState(token: String, name: String, stream: String, partition: Int): Future[Long] = ???

  override def putTransactionData(token: String, stream: String, partition: Int, transaction: Long, from: Int, to: Int, data: Seq[ByteBuffer]): Future[Boolean] = ???

  override def getTransactionData(token: String, stream: String, partition: Int, transaction: Long, from: Int, to: Int): Future[Seq[ByteBuffer]] = ???
}
