package com.bwsw.tstreams.entities

import java.util
import java.util.UUID
import com.datastax.driver.core.{Row, Session}


/**
 * Transactions settings
 * @param txnUuid Time of transaction
 * @param totalItems Total packets in transaction
 * @param ttl Transaction expiration time in seconds
 */
case class TransactionSettings(txnUuid : UUID, totalItems : Int, ttl : Int)

/**
 * Metadata entity for commits
 * @param commitLog Table name in C*
 * @param session Session to use for this entity
 */
class CommitEntity(commitLog : String, session: Session) {
  private val commitStatement = session
    .prepare(s"insert into $commitLog (stream,partition,transaction,cnt) values(?,?,?,?) USING TTL ?")

  private val selectTransactionsMoreThanStatement = session
    .prepare(s"select transaction,cnt,TTL(cnt) from $commitLog where stream=? AND partition=? AND transaction>? LIMIT ?")

  private val selectTransactionsMoreThanStatementWithoutLimit = session
    .prepare(s"select transaction,cnt,TTL(cnt) from $commitLog where stream=? AND partition=? AND transaction>?")

  private val selectTransactionsMoreThanAndLessOrEqualThanStatement = session
    .prepare(s"select transaction,cnt,TTL(cnt) from $commitLog where stream=? AND partition=? AND transaction>? AND transaction <= ?")

  private val selectTransactionsLessThanStatement = session
    .prepare(s"select transaction,cnt,TTL(cnt) from $commitLog where stream=? AND partition=? AND transaction<? LIMIT ?")

  private val selectTransactionAmountStatement = session
    .prepare(s"select cnt,TTL(cnt) from $commitLog where stream=? AND partition=? AND transaction=? LIMIT 1")
  
  /**
   * Closing some specific transaction
   * @param streamName name of the stream
   * @param partition number of partition
   * @param transaction transaction unique id
   * @param totalCnt total amount of pieces of data in concrete transaction
   * @param ttl time of transaction existence in seconds
   */
  def commit(streamName : String, partition : Int, transaction: UUID, totalCnt : Int, ttl : Int) : Unit = {
    val values = List(streamName, new Integer(partition), transaction, new Integer(totalCnt), new Integer(ttl))
    val statementWithBindings = commitStatement.bind(values:_*)
    session.execute(statementWithBindings)
  }

  /**
   * Retrieving some set of transactions more than last transaction (if cnt is default will be no limit to retrieve)
   * @param streamName Name of the stream
   * @param partition Number of the partition
   * @param lastTransaction Transaction from which start to retrieve
   * @param cnt Amount of retrieved queue (can be less than cnt in case of insufficiency of transactions)
   * @return Queue of selected transactions
   */
  def getTransactions(streamName : String, partition : Int, lastTransaction : UUID, cnt : Int = -1) : scala.collection.mutable.Queue[TransactionSettings] = {
    val statementWithBindings =
      if (cnt == -1) {
        val values : List[AnyRef] = List(streamName, new Integer(partition), lastTransaction)
        selectTransactionsMoreThanStatementWithoutLimit.bind(values: _*)
      }
      else {
        val values : List[AnyRef] = List(streamName, new Integer(partition), lastTransaction, new Integer(cnt))
        selectTransactionsMoreThanStatement.bind(values: _*)
      }

    val selected = session.execute(statementWithBindings)

    val q = scala.collection.mutable.Queue[TransactionSettings]()
    val it = selected.iterator()
    while(it.hasNext){
      val value = it.next()
      q.enqueue(TransactionSettings(value.getUUID("transaction"), value.getInt("cnt"), value.getInt("ttl(cnt)")))
    }
    q
  }

  /**
   * Retrieving some set of transactions(used only by getLastTransaction)
   * @param streamName Name of the stream
   * @param partition Number of the partition
   * @param lastTransaction Transaction from which start to retrieve
   * @param cnt Amount of retrieved queue (can be less than cnt in case of insufficiency of transactions)
   * @return Queue of selected transactions
   */
  def getLastTransactionHelper(streamName : String, partition : Int, lastTransaction : UUID, cnt : Int=128) : scala.collection.mutable.Queue[TransactionSettings] = {
    val values : List[AnyRef] = List(streamName, new Integer(partition), lastTransaction, new Integer(cnt))
    val statementWithBindings = selectTransactionsLessThanStatement.bind(values:_*)
    val selected = session.execute(statementWithBindings)

    val q = scala.collection.mutable.Queue[TransactionSettings]()
    val it = selected.iterator()
    while(it.hasNext){
      val value = it.next()
      q.enqueue(TransactionSettings(value.getUUID("transaction"), value.getInt("cnt"), value.getInt("ttl(cnt)")))
    }
    q.reverse
  }


  /**
   * Retrieving some set of transactions between bounds (L,R]
   * @param streamName Name of the stream
   * @param partition Number of the partition
   * @param leftBorder Left border of transactions to consume
   * @param rightBorder Right border of transactions to consume
   * @return Iterator of selected transactions
   */
  def getTransactionsIterator(streamName : String, partition : Int, leftBorder : UUID, rightBorder : UUID) : util.Iterator[Row] = {
    val values : List[AnyRef] = List(streamName, new Integer(partition), leftBorder, rightBorder)
    val statementWithBindings = selectTransactionsMoreThanAndLessOrEqualThanStatement.bind(values:_*)
    val selected = session.execute(statementWithBindings)
    val it = selected.iterator()
    it
  }



  /**
   * Retrieving only one concrete transaction amount and ttl
   * @param streamName Name of concrete stream
   * @param partition Number of partition
   * @param transaction Concrete transaction time
   * @return Amount of data in concrete transaction and ttl
   */
  def getTransactionAmount(streamName : String, partition : Int, transaction : UUID) : Option[(Int,Int)] = {
    val values : List[AnyRef] = List(streamName, new Integer(partition), transaction)
    val statementWithBindings = selectTransactionAmountStatement.bind(values:_*)

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
