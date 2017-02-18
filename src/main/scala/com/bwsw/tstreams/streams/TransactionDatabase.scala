package com.bwsw.tstreams.streams

import java.lang.Long
import java.util.concurrent.atomic.AtomicInteger

import com.bwsw.tstreams.common.StorageClient
import transactionService.rpc.{ProducerTransaction, TransactionStates}

import scala.annotation.tailrec
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success}


case class TransactionRecord(partition: Int, transactionID: Long, state: transactionService.rpc.TransactionStates, count: Int, ttl: Long)

object TransactionDatabase {
  def AGGREGATION_INTERVAL = SCALE * AGGREGATION_FACTOR

  var SCALE = 10000
  var AGGREGATION_FACTOR = 1000
  /* second */
  var ACTIVITY_CACHE_SIZE = 10000

  var READ_SINGLE_OP_TIMEOUT = 1.minute
  var READ_MULTIPLE_OP_TIMEOUT = 10.minute


  def getAggregationInterval(transactionID: Long, interval: Int = TransactionDatabase.AGGREGATION_INTERVAL): Long =
    Math.floorDiv(transactionID, interval)

}

/**
  * Created by Ivan Kudryavtsev on 24.09.16.
  */
class TransactionDatabase(storageClient: StorageClient, stream: String) {

  val resourceCounter = new AtomicInteger(0)

  def getResourceCounter() = resourceCounter.get()

  def getSession() = storageClient

  def put[T](transaction: TransactionRecord)(onComplete: TransactionRecord => T) = {
    val s = stream
    val f = storageClient.client.putTransaction(new ProducerTransaction {
      override def stream: String = s
      override def partition: Int = transaction.partition
      override def transactionID: Long = transaction.transactionID
      override def state: TransactionStates = transaction.state
      override def quantity: Int = transaction.count
      override def keepAliveTTL: Long = transaction.ttl
    })

    f onComplete {
      case Success(res) => res
      case Failure(t) => throw t
    }
  }

  def get(partition: Integer, transactionID: Long): Option[TransactionRecord] = {
    val txnSeq = Await.result(storageClient.client.scanTransactions(stream, partition, transactionID, transactionID), TransactionDatabase.READ_SINGLE_OP_TIMEOUT)
    txnSeq.headOption.map(t => TransactionRecord(partition, transactionID, t.state, t.quantity,  t.keepAliveTTL))
  }

  private def scanTransactionsInterval(partition: Integer, interval: Long): Seq[TransactionRecord] = {
    val txnSeq = Await.result(storageClient.client.scanTransactions(stream, partition, interval * TransactionDatabase.AGGREGATION_INTERVAL, (interval+1) * TransactionDatabase.AGGREGATION_INTERVAL - 1), TransactionDatabase.READ_MULTIPLE_OP_TIMEOUT)
    txnSeq.map(t => TransactionRecord(partition, t.transactionID, t.state, t.quantity, t.keepAliveTTL))
  }

  @tailrec
  private def scanForwardInt(partition: Integer, transactionFrom: Long, transactionTo: Long)(interval: Long, intervalDeadHigh: Long, list: List[TransactionRecord], predicate: TransactionRecord => Boolean): List[TransactionRecord] = {

    if (interval > intervalDeadHigh)
      return list

    val intervalTransactions = scanTransactionsInterval(partition, interval)

    val candidateTransactions = intervalTransactions
      .filter(rec => rec.transactionID >= transactionFrom && rec.transactionID <= transactionTo)

    val filteredTransactions = candidateTransactions.takeWhile(predicate)
    if (candidateTransactions.size == filteredTransactions.size)
      scanForwardInt(partition, transactionFrom, transactionTo)(interval + 1, intervalDeadHigh, list ++ candidateTransactions, predicate)
    else
      list ++ filteredTransactions
  }

  def takeWhileForward(partition: Integer, transactionFrom: Long, deadHigh: Long)(predicate: TransactionRecord => Boolean): List[TransactionRecord] = {
    val intervalFrom = TransactionDatabase.getAggregationInterval(transactionFrom)
    val intervalDeadHi = TransactionDatabase.getAggregationInterval(deadHigh)
    scanForwardInt(partition, transactionFrom, deadHigh)(intervalFrom, intervalDeadHi, Nil, predicate)
  }

  @tailrec
  private def scanBackwardInt(partition: Integer, transactionFrom: Long, transactionTo: Long)(interval: Long, intervalDeadLow: Long, list: List[TransactionRecord], predicate: TransactionRecord => Boolean): List[TransactionRecord] = {

    if (interval < intervalDeadLow)
      return list

    val intervalTransactions = scanTransactionsInterval(partition, interval)

    val candidateTransactions = intervalTransactions
      .filter(rec => rec.transactionID >= transactionTo && rec.transactionID <= transactionFrom)
      .reverse

    val filteredTransactions = candidateTransactions.takeWhile(predicate)
    if (candidateTransactions.size == filteredTransactions.size)
      scanBackwardInt(partition, transactionFrom, transactionTo)(interval - 1, intervalDeadLow, list ++ candidateTransactions, predicate)
    else
      list ++ filteredTransactions
  }

  def takeWhileBackward(partition: Integer, transactionFrom: Long, deadLow: Long)(predicate: TransactionRecord => Boolean): List[TransactionRecord] = {
    if (deadLow > transactionFrom)
      return Nil

    val intervalFrom = TransactionDatabase.getAggregationInterval(transactionFrom)
    val intervalDeadLow = TransactionDatabase.getAggregationInterval(deadLow)
    scanBackwardInt(partition, transactionFrom, deadLow)(intervalFrom, intervalDeadLow, Nil, predicate)
  }

  @tailrec
  private def searchBackwardInt(partition: Integer, transactionFrom: Long, transactionTo: Long)(interval: Long, intervalDeadLow: Long, predicate: (TransactionRecord) => Boolean): Option[TransactionRecord] = {
    if (interval < intervalDeadLow)
      return None

    val intervalTransactions = scanTransactionsInterval(partition, interval)

    val findOpt = intervalTransactions
      .filter(rec => rec.transactionID >= transactionTo && rec.transactionID <= transactionFrom)
      .reverse.find(predicate)

    if (findOpt.nonEmpty)
      findOpt
    else
      searchBackwardInt(partition, transactionFrom, transactionTo)(interval - 1, intervalDeadLow, predicate)
  }

  def searchBackward(partition: Integer, transactionFrom: Long, deadLow: Long)(predicate: TransactionRecord => Boolean): Option[TransactionRecord] = {
    if (deadLow > transactionFrom)
      return None

    val intervalTo = TransactionDatabase.getAggregationInterval(transactionFrom)
    val intervalDeadLow = TransactionDatabase.getAggregationInterval(deadLow)
    searchBackwardInt(partition, transactionFrom, deadLow)(intervalTo, intervalDeadLow, predicate)
  }

}