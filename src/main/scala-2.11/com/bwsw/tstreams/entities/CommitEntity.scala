package com.bwsw.tstreams.entities

import java.util
import java.util.UUID
import java.util.concurrent.Executor

import com.datastax.driver.core.{ResultSet, Row, Session}
import com.google.common.util.concurrent.{FutureCallback, Futures}
import com.bwsw.tstreams.agents.consumer.Transaction



/**
  * Metadata entity for commits
  *
  * @param commitLog Table name in C*
  * @param session   Session to use for this entity
  */
class CommitEntity(commitLog: String, session: Session) {
  private val commitStatement = session
    .prepare(s"INSERT INTO $commitLog (stream,partition,transaction,cnt) VALUES (?,?,?,?) USING TTL ?")

  private val deleteStatement = session
    .prepare(s"DELETE FROM $commitLog WHERE stream = ? AND partition = ? AND transaction = ?")


  private val selectTransactionsMoreThanStatement = session
    .prepare(s"SELECT transaction,cnt,TTL(cnt) FROM $commitLog WHERE stream = ? AND partition = ? AND transaction > ? LIMIT ?")

  private val selectTransactionsMoreThanStatementWithoutLimit = session
    .prepare(s"SELECT transaction,cnt,TTL(cnt) FROM $commitLog WHERE stream = ? AND partition = ? AND transaction > ?")

  private val selectTransactionsMoreThanAndLessOrEqualThanStatement = session
    .prepare(s"SELECT transaction,cnt,TTL(cnt) FROM $commitLog WHERE stream = ? AND partition = ? AND transaction > ? AND transaction <= ?")

  private val selectTransactionsLessThanStatement = session
    .prepare(s"SELECT transaction,cnt,TTL(cnt) FROM $commitLog WHERE stream = ? AND partition = ? AND transaction < ? LIMIT ?")

  private val selectTransactionAmountStatement = session
    .prepare(s"SELECT cnt,TTL(cnt) FROM $commitLog WHERE stream = ? AND partition = ? AND transaction = ? LIMIT 1")

  /**
    * Closing some specific transaction
    *
    * @param streamName  name of the stream
    * @param partition   number of partition
    * @param transaction transaction unique id
    * @param totalCnt    total amount of pieces of data in concrete transaction
    * @param ttl         time of transaction existence in seconds
    */
  def commit(streamName: String, partition: Int, transaction: UUID, totalCnt: Int, ttl: Int): Unit = {
    val values = List(streamName, new Integer(partition), transaction, new Integer(totalCnt), new Integer(ttl))
    val statementWithBindings = commitStatement.bind(values: _*)
    session.execute(statementWithBindings)
  }

  /**
    * Does asynchronous commit to C*
    *
    * @param streamName  name of stream
    * @param partition
    * @param transaction UUID
    * @param totalCnt    amount of values
    * @param ttl
    * @return
    */
  def commitAsync(streamName: String,
                  partition: Int,
                  transaction: UUID,
                  totalCnt: Int,
                  ttl: Int,
                  executor: Executor,
                  function: () => Unit): Unit = {
    val values = List(streamName, new Integer(partition), transaction, new Integer(totalCnt), new Integer(ttl))
    val statementWithBindings = commitStatement.bind(values: _*)
    val f = session.executeAsync(statementWithBindings)
    Futures.addCallback(f, new FutureCallback[ResultSet]() {
      override def onSuccess(r: ResultSet) = {
        function()
      }

      override def onFailure(r: Throwable) = {
        throw new IllegalStateException("PostCommit Callback execution failed! Wrong state!")
      }
    }, executor)
  }

  /**
    * Does asynchronous delete to C*
    *
    * @param streamName  name of stream
    * @param partition
    * @param transaction UUID
    * @return
    */
  def deleteAsync(streamName: String,
                  partition: Int,
                  transaction: UUID,
                  executor: Executor,
                  function: () => Unit): Unit = {
    val values = List(streamName, new Integer(partition), transaction)
    val statementWithBindings = deleteStatement.bind(values: _*)
    val f = session.executeAsync(statementWithBindings)
    Futures.addCallback(f, new FutureCallback[ResultSet]() {
      override def onSuccess(r: ResultSet) = {
        function()
      }

      override def onFailure(r: Throwable) = {
        throw new IllegalStateException("Delete Callback execution failed! Wrong state!")
      }
    }, executor)
  }


  /**
    * Retrieving some set of transactions more than last transaction (if cnt is default will be no limit to retrieve)
    *
    * @param streamName      Name of the stream
    * @param partition       Number of the partition
    * @param fromTransaction Transaction from which start to retrieve
    * @param cnt             Amount of retrieved queue (can be less than cnt in case of insufficiency of transactions)
    * @return Queue of selected transactions
    */
  def getTransactions[T](streamName: String, partition: Int, fromTransaction: UUID, cnt: Int = -1): scala.collection.mutable.Queue[Transaction[T]] = {
    val statementWithBindings =
      if (cnt == -1) {
        val values: List[AnyRef] = List(streamName, new Integer(partition), fromTransaction)
        selectTransactionsMoreThanStatementWithoutLimit.bind(values: _*)
      }
      else {
        val values: List[AnyRef] = List(streamName, new Integer(partition), fromTransaction, new Integer(cnt))
        selectTransactionsMoreThanStatement.bind(values: _*)
      }

    val selected = session.execute(statementWithBindings)

    val q = scala.collection.mutable.Queue[Transaction[T]]()
    val it = selected.iterator()
    while (it.hasNext) {
      val value = it.next()
      q.enqueue(new Transaction[T](partition, value.getUUID("transaction"), value.getInt("cnt"), value.getInt("ttl(cnt)")))
    }
    q
  }

  /**
    * Retrieving some set of transactions(used only by getLastTransaction)
    *
    * @param streamName      Name of the stream
    * @param partition       Number of the partition
    * @param lastTransaction Transaction from which start to retrieve
    * @param cnt             Amount of retrieved queue (can be less than cnt in case of insufficiency of transactions)
    * @return Queue of selected transactions
    */
  def getLastTransactionHelper[T](streamName: String, partition: Int, lastTransaction: UUID, cnt: Int = 128): scala.collection.mutable.Queue[Transaction[T]] = {
    val values: List[AnyRef] = List(streamName, new Integer(partition), lastTransaction, new Integer(cnt))
    val statementWithBindings = selectTransactionsLessThanStatement.bind(values: _*)
    val selected = session.execute(statementWithBindings)

    val q = scala.collection.mutable.Queue[Transaction[T]]()
    val it = selected.iterator()
    while (it.hasNext) {
      val value = it.next()
      q.enqueue(new Transaction(partition, value.getUUID("transaction"), value.getInt("cnt"), value.getInt("ttl(cnt)")))
    }
    q.reverse
  }


  /**
    * Retrieving some set of transactions between bounds (L,R]
    *
    * @param streamName  Name of the stream
    * @param partition   Number of the partition
    * @param leftBorder  Left border of transactions to consume
    * @param rightBorder Right border of transactions to consume
    * @return Iterator of selected transactions
    */
  def getTransactionsIterator(streamName: String, partition: Int, leftBorder: UUID, rightBorder: UUID): util.Iterator[Row] = {
    val values: List[AnyRef] = List(streamName, new Integer(partition), leftBorder, rightBorder)
    val statementWithBindings = selectTransactionsMoreThanAndLessOrEqualThanStatement.bind(values: _*)
    val selected = session.execute(statementWithBindings)
    val it = selected.iterator()
    it
  }


  /**
    * Retrieving only one concrete transaction item count and ttl
    *
    * @param streamName  Name of concrete stream
    * @param partition   Number of partition
    * @param transaction Concrete transaction time
    * @return Amount of data in concrete transaction and ttl
    */
  def getTransactionItemCountAndTTL(streamName: String, partition: Int, transaction: UUID): Option[(Int, Int)] = {
    val values: List[AnyRef] = List(streamName, new Integer(partition), transaction)
    val statementWithBindings = selectTransactionAmountStatement.bind(values: _*)

    val selected = session.execute(statementWithBindings)

    val list: util.List[Row] = selected.all()
    if (list.isEmpty)
      None
    else {
      val settings = list.get(0)
      Some(settings.getInt("cnt"), settings.getInt("ttl(cnt)"))
    }
  }
}
