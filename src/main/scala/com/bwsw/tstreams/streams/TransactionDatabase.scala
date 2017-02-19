package com.bwsw.tstreams.streams

import java.util.concurrent.atomic.AtomicInteger

import com.bwsw.tstreams.common.StorageClient
import org.slf4j.LoggerFactory

import scala.annotation.tailrec

case class TransactionRecord(partition: Int, transactionID: Long, state: transactionService.rpc.TransactionStates, count: Int, ttl: Long)

object TransactionDatabase {
  var SCALE = 10000
  var AGGREGATION_FACTOR = 1000

  val logger = LoggerFactory.getLogger(this.getClass)

  def AGGREGATION_INTERVAL = SCALE * AGGREGATION_FACTOR

  def getAggregationInterval(transactionID: Long, interval: Int = TransactionDatabase.AGGREGATION_INTERVAL): Long =
    Math.floorDiv(transactionID, interval)




}

/**
  * Created by Ivan Kudryavtsev on 24.09.16.
  */
class TransactionDatabase(storageClient: StorageClient, streamName: String) {

  val resourceCounter = new AtomicInteger(0)

  def getResourceCounter() = resourceCounter.get()

  def getSession() = storageClient

  @tailrec
  private def scanForwardInt(partition: Integer, transactionFrom: Long, transactionTo: Long)(interval: Long, intervalDeadHigh: Long, list: List[TransactionRecord], predicate: TransactionRecord => Boolean): List[TransactionRecord] = {
    TransactionDatabase.logger.info(s"scanForwardInt(${partition},${transactionFrom},${transactionTo})(${interval},${intervalDeadHigh},${list})")
    if (interval > intervalDeadHigh)
      return list

    val intervalTransactions = storageClient.scanTransactionsInterval(streamName, partition, interval)

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

    val intervalTransactions = storageClient.scanTransactionsInterval(streamName, partition, interval)

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

    val intervalTransactions = storageClient.scanTransactionsInterval(streamName, partition, interval)

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