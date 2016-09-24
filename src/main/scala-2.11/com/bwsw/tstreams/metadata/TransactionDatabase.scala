package com.bwsw.tstreams.metadata

import com.datastax.driver.core.Session

import scala.collection.mutable


case class TransactionRecord(partition: Int, transaction: Long, count: Int, ttl: Int)

/**
  * Created by Ivan Kudryavtsev on 24.09.16.
  */
class TransactionDatabase {

  val activityCache = mutable.HashSet[Long]()

  def init(session: Session, stream: String) = {

  }

  def put[T](partition: Int, transaction: Long, count: Int, ttl: Int, onComplete: => T) = {
    onComplete
  }

  def get(partition: Int, transaction: Long): TransactionRecord = {
    null
  }

  def scanForward[T](partition: Int, transactionFrom: Long, predicate: (TransactionRecord) => Boolean): List[TransactionRecord] = {
    Nil
  }

  def scanBackward[T](partition: Int, transactionTo: Long, predicate: (TransactionRecord) => Boolean): List[TransactionRecord] = {
    Nil
  }

  def scanBetween[T](partition: Int, transactionFrom: Long, transactionTo: Long, predicate: (TransactionRecord) => Boolean): List[TransactionRecord] = {
    Nil
  }
}
