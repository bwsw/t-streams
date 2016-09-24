package com.bwsw.tstreams.metadata

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ExecutorService, ConcurrentHashMap}

import com.datastax.driver.core.{ResultSet, Session}
import java.lang.Long
import scala.collection.JavaConverters._

import com.google.common.util.concurrent.{FutureCallback, Futures}

case class TransactionRecord(partition: Int, transactionID: Long, count: Int, ttl: Int)

object TransactionDatabase {
  def AGGREGATION_INTERVAL = SCALE * AGGREGATION_FACTOR
  var SCALE                = 10000
  var AGGREGATION_FACTOR   = 100
  def getAggregationInterval(transactionID: Long): Long = Math.floorDiv(transactionID, TransactionDatabase.AGGREGATION_INTERVAL)
}

/**
  * Created by Ivan Kudryavtsev on 24.09.16.
  */
class TransactionDatabase(session: Session, stream: String) {

  val activityCache = new ConcurrentHashMap[(Int, Long), Boolean]()
  val statements = RequestsRepository.getStatements(session)
  val resourceCounter = new AtomicInteger(0)

  def getResourceCounter() = resourceCounter.get()
  def getSession() = session

  def updateActivityCache(partition: Int, interval: Long) = {
    val boundStatement = statements.activityPutStatement.bind(List(stream, new Integer(partition), new Long(interval)): _*)
    session.execute(boundStatement)
    activityCache.put((partition, interval), true)
  }

  def del[T](partition: Integer, transactionID: Long) = {
    val interval = TransactionDatabase.getAggregationInterval(transactionID)
    val boundStatement = statements.commitLogDeleteStatement.bind(List(stream, new Integer(partition), new Long(interval), new Long(transactionID)): _*)
    session.execute(boundStatement)
  }

  def put[T](transaction: TransactionRecord, asynchronousExecutor: ExecutorService)(onComplete: TransactionRecord => T) = {
    val interval = TransactionDatabase.getAggregationInterval(transaction.transactionID)
    if((!activityCache.contains((transaction.partition, interval)) || !activityCache.get((transaction.partition, interval)))
      && transaction.count > 0)
      updateActivityCache(transaction.partition, interval)
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

  def isActivityWas(partition: Integer, interval: Long): Boolean = {
    syncActivityCache(partition, interval)
    activityCache.get((partition, interval))
  }

  def syncActivityCache(partition: Integer, interval: Long) = {
    val key = (partition, interval)
    if(!activityCache.contains(key) || !activityCache.get(key)) {
      val boundStatement = statements.activityGetStatement.bind(List(stream, partition, interval): _*)
      val activityRow = session.execute(boundStatement)
      val row = activityRow.one()
      val activityStatus = if (row == null) false else true
      activityCache.put((partition, interval), activityStatus)
    }
  }

  def get(partition: Integer, transactionID: Long): Option[TransactionRecord] = {
    val interval = TransactionDatabase.getAggregationInterval(transactionID)
    if(isActivityWas(partition, interval)) {
      val boundStatement = statements.commitLogGetStatement.bind(List(stream, partition, interval, transactionID): _*)
      val activityRow = session.execute(boundStatement)
      val row = activityRow.one()
      if(row != null)
        Some(TransactionRecord(partition = partition, transactionID = transactionID,
          count = row.getInt("count"), ttl = row.getInt("ttl(count)")))
      else
        None
    } else
      None
  }

  def getTransactionsForInterval(partition: Integer, interval: Long): List[TransactionRecord] = {
    if(isActivityWas(partition, interval)) {
      val boundStatement = statements.commitLogScanStatement.bind(List(stream, partition, interval): _*)
      val activityRows = session.execute(boundStatement)
      activityRows.iterator().asScala
        .map(row => TransactionRecord(partition = partition, transactionID = row.getLong("transaction"),
          count = row.getInt("count"), ttl = row.getInt("ttl(count)"))).toList
    } else Nil
  }

  private def scanForwardInt(partition: Integer, interval: Long, deadHigh: Long, list: List[TransactionRecord], predicate: TransactionRecord => Boolean): List[TransactionRecord] = {
    if(interval > deadHigh) return list
    val candidateTransactions = getTransactionsForInterval(partition, interval)
    val filteredTransactions = candidateTransactions.takeWhile(predicate)
    if(candidateTransactions.size == filteredTransactions.size)
       scanForwardInt(partition, interval + 1, deadHigh, list ++ candidateTransactions, predicate)
    else
      list ++ filteredTransactions
  }

  def scanForward(partition: Integer, transactionFrom: Long, deadHigh: Long)(predicate: TransactionRecord => Boolean): List[TransactionRecord] = {
    val intervalFrom = TransactionDatabase.getAggregationInterval(transactionFrom)
    val intervalDeadHi = TransactionDatabase.getAggregationInterval(deadHigh)
    scanForwardInt(partition, intervalFrom, intervalDeadHi, Nil, predicate)
  }

  private def scanBackwardInt(partition: Integer, interval: Long, deadLow: Long, list: List[TransactionRecord], predicate: TransactionRecord => Boolean): List[TransactionRecord] = {
    if(interval < deadLow) return list
    val candidateTransactions = getTransactionsForInterval(partition, interval).reverse
    val filteredTransactions = candidateTransactions.takeWhile(predicate)
    if(candidateTransactions.size == filteredTransactions.size)
      scanBackwardInt(partition, interval - 1, deadLow, list ++ candidateTransactions, predicate)
    else
      list ++ filteredTransactions
  }

  def scanBackward(partition: Integer, transactionTo: Long, deadLow: Long)(predicate: TransactionRecord => Boolean): List[TransactionRecord] = {
    val intervalTo = TransactionDatabase.getAggregationInterval(transactionTo)
    val intervalDeadLow = TransactionDatabase.getAggregationInterval(deadLow)
    scanBackwardInt(partition, intervalTo, intervalDeadLow, Nil, predicate)
  }

  def searchBackwardInt(partition: Integer, interval: Long, deadLow: Long, predicate: (TransactionRecord) => Boolean): Option[TransactionRecord] = {
    if(interval < deadLow) return None
    val candidateTransactions = getTransactionsForInterval(partition, interval).reverse
    val findOpt = candidateTransactions.find(predicate)
    if(findOpt.nonEmpty)
      findOpt
    else
      searchBackwardInt(partition, interval - 1, deadLow, predicate)
  }

  def searchBackward(partition: Integer, transactionTo: Long, deadLow: Long)(predicate: TransactionRecord => Boolean): Option[TransactionRecord] = {
    val intervalTo = TransactionDatabase.getAggregationInterval(transactionTo)
    val intervalDeadLow = TransactionDatabase.getAggregationInterval(deadLow)
    searchBackwardInt(partition, intervalTo, intervalDeadLow, predicate)
  }

}
