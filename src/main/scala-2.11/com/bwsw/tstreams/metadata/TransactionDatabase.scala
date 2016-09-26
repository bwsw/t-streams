package com.bwsw.tstreams.metadata

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ExecutorService}

import com.datastax.driver.core.{ResultSet, Session}
import java.lang.Long

import scala.collection.JavaConverters._
import com.google.common.util.concurrent.{FutureCallback, Futures}

import scala.annotation.tailrec

case class TransactionRecord(partition: Int, transactionID: Long, count: Int, ttl: Int)

object TransactionDatabase {
  def AGGREGATION_INTERVAL            = SCALE * AGGREGATION_FACTOR
  var SCALE                           = 10000
  var AGGREGATION_FACTOR              = 1000    /* second */
  var ACTIVITY_CACHE_SIZE             = 10000
  def getAggregationInterval(transactionID: Long, interval: Int = TransactionDatabase.AGGREGATION_INTERVAL): Long = Math.floorDiv(transactionID, interval)

}

/**
  * Created by Ivan Kudryavtsev on 24.09.16.
  */
class TransactionDatabase(session: Session, stream: String) {

  val statements = RequestsRepository.getStatements(session)
  val resourceCounter = new AtomicInteger(0)

  def getResourceCounter() = resourceCounter.get()
  def getSession() = session

  def del(partition: Integer, transactionID: Long) = {
    val interval = TransactionDatabase.getAggregationInterval(transactionID)
    val boundStatement = statements.commitLogDeleteStatement.bind(List(stream, new Integer(partition), new Long(interval), new Long(transactionID)): _*)
    session.execute(boundStatement)
  }

  def put[T](transaction: TransactionRecord, asynchronousExecutor: ExecutorService)(onComplete: TransactionRecord => T) = {
    val interval = TransactionDatabase.getAggregationInterval(transaction.transactionID)

    val boundStatement = statements.commitLogPutStatement.bind(List(stream, new Integer(transaction.partition), new Long(interval), new Long(transaction.transactionID), new Integer(transaction.count), new Integer(transaction.ttl)): _*)

    resourceCounter.incrementAndGet()
    val future = session.executeAsync(boundStatement)
    Futures.addCallback(future, new FutureCallback[ResultSet]() {
      override def onSuccess(r: ResultSet) = {
        resourceCounter.decrementAndGet()
        onComplete(transaction)
      }
      override def onFailure(r: Throwable) = throw new IllegalStateException("Callback onComplete execution failed! Wrong state!")
    }, asynchronousExecutor)
  }

  def get(partition: Integer, transactionID: Long): Option[TransactionRecord] = {
    val interval = TransactionDatabase.getAggregationInterval(transactionID)
    val boundStatement = statements.commitLogGetStatement.bind(List(stream, partition, interval, transactionID): _*)
    val activityRow = session.execute(boundStatement)
    Option(activityRow.one()).map(row => TransactionRecord(partition,transactionID,row.getInt("count"),row.getInt("ttl(count)")))
  }

  private def getTransactionsForInterval(partition: Integer, interval: Long): List[TransactionRecord] = {
    val boundStatement = statements.commitLogScanStatement.bind(List(stream, partition, interval): _*)
    val activityRows = session.execute(boundStatement)
    activityRows.iterator().asScala
      .map(row => TransactionRecord(partition = partition, transactionID = row.getLong("transaction"),
        count = row.getInt("count"), ttl = row.getInt("ttl(count)"))).toList
  }

  @tailrec
  private def scanForwardInt(partition: Integer, transactionFrom: Long, transactionTo: Long)(interval: Long, intervalDeadHigh: Long, list: List[TransactionRecord], predicate: TransactionRecord => Boolean): List[TransactionRecord] = {

    if(interval > intervalDeadHigh)
      return list

    val intervalTransactions = getTransactionsForInterval(partition, interval)

    val candidateTransactions = intervalTransactions
      .filter(rec => rec.transactionID >= transactionFrom && rec.transactionID <= transactionTo)

    val filteredTransactions = candidateTransactions.takeWhile(predicate)
    if(candidateTransactions.size == filteredTransactions.size)
       scanForwardInt(partition, transactionFrom, transactionTo) (interval + 1, intervalDeadHigh, list ++ candidateTransactions, predicate)
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

    if(interval < intervalDeadLow)
      return list

    val intervalTransactions = getTransactionsForInterval(partition, interval)

    val candidateTransactions = intervalTransactions
      .filter(rec => rec.transactionID >= transactionTo && rec.transactionID <= transactionFrom)
      .reverse

    val filteredTransactions = candidateTransactions.takeWhile(predicate)
    if(candidateTransactions.size == filteredTransactions.size)
      scanBackwardInt(partition, transactionFrom, transactionTo) (interval - 1, intervalDeadLow, list ++ candidateTransactions, predicate)
    else
      list ++ filteredTransactions
  }

  def takeWhileBackward(partition: Integer, transactionFrom: Long, deadLow: Long)(predicate: TransactionRecord => Boolean): List[TransactionRecord] = {
    if(deadLow > transactionFrom)
      return Nil

    val intervalFrom = TransactionDatabase.getAggregationInterval(transactionFrom)
    val intervalDeadLow = TransactionDatabase.getAggregationInterval(deadLow)
    scanBackwardInt(partition, transactionFrom, deadLow)(intervalFrom, intervalDeadLow, Nil, predicate)
  }

  @tailrec
  private def searchBackwardInt(partition: Integer, transactionFrom: Long, transactionTo: Long)(interval: Long, intervalDeadLow: Long, predicate: (TransactionRecord) => Boolean): Option[TransactionRecord] = {
    if(interval < intervalDeadLow)
      return None

    val intervalTransactions = getTransactionsForInterval(partition, interval)

    val findOpt = intervalTransactions
      .filter(rec => rec.transactionID >= transactionTo && rec.transactionID <= transactionFrom)
      .reverse.find(predicate)

    if(findOpt.nonEmpty)
      findOpt
    else
      searchBackwardInt(partition, transactionFrom, transactionTo)(interval - 1, intervalDeadLow, predicate)
  }

  def searchBackward(partition: Integer, transactionFrom: Long, deadLow: Long)(predicate: TransactionRecord => Boolean): Option[TransactionRecord] = {
    if(deadLow > transactionFrom)
      return None

    val intervalTo = TransactionDatabase.getAggregationInterval(transactionFrom)
    val intervalDeadLow = TransactionDatabase.getAggregationInterval(deadLow)
    searchBackwardInt(partition, transactionFrom, deadLow)(intervalTo, intervalDeadLow, predicate)
  }

}
