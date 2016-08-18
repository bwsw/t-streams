package com.bwsw.tstreams.entities

import java.util.UUID

import com.datastax.driver.core.{BatchStatement, Session}

/**
  * Consumer entity for interact with consumers metadata
  *
  * @param entityName Metadata table name
  * @param session    Session with metadata
  */
class ConsumerEntity(entityName: String, session: Session) {

  /**
    * Statement for check exist or not some specific consumer
    */
  private val existStatement = session
    .prepare(s"SELECT name FROM $entityName WHERE name=? LIMIT 1")

  /**
    * Statement for saving single offset
    */
  private val saveSingleOffsetStatement = session
    .prepare(s"INSERT INTO $entityName (name,stream,partition,last_transaction) VALUES(?,?,?,?)")

  /**
    * Statement for retrieving offsets from consumers metadata
    */
  private val getOffsetStatement = session
    .prepare(s"SELECT last_transaction FROM $entityName WHERE name=? AND stream=? AND partition=? LIMIT 1")


  /**
    * Checking exist or not concrete consumer
    *
    * @param consumerName Name of the consumer
    * @return Exist or not concrete consumer
    */
  def exists(consumerName: String): Boolean = {
    val statementWithBindings = existStatement.bind(consumerName)
    val res = session.execute(statementWithBindings).all()
    !res.isEmpty
  }

  /**
    * Saving offset batch
    *
    * @param name                Name of the consumer
    * @param stream              Name of the specific stream
    * @param partitionAndLastTxn Set of partition and last transaction pairs to save
    */
  def saveBatchOffset(name: String, stream: String, partitionAndLastTxn: scala.collection.mutable.Map[Int, UUID]): Unit = {
    val batchStatement = new BatchStatement()
    partitionAndLastTxn.map { case (partition, lastTxn) =>
      val values: List[AnyRef] = List(name, stream, new Integer(partition), lastTxn)
      val statementWithBindings = saveSingleOffsetStatement.bind(values: _*)
      batchStatement.add(statementWithBindings)
    }
    session.execute(batchStatement)
  }

  /**
    * Saving single offset
    *
    * @param name      Name of the specific consumer
    * @param stream    Name of the specific stream
    * @param partition Name of the specific partition
    * @param offset    Offset to save
    */
  def saveSingleOffset(name: String, stream: String, partition: Int, offset: UUID): Unit = {
    val values: List[AnyRef] = List(name, stream, new Integer(partition), offset)
    val statementWithBindings = saveSingleOffsetStatement.bind(values: _*)
    session.execute(statementWithBindings)
  }

  /**
    * Retrieving specific offset for particular consumer
    *
    * @param name      Name of the specific consumer
    * @param stream    Name of the specific stream
    * @param partition Name of the specific partition
    * @return Offset
    */
  def getLastSavedOffset(name: String, stream: String, partition: Int): UUID = {
    val values = List(name, stream, new Integer(partition))
    val statementWithBindings = getOffsetStatement.bind(values: _*)
    val selected = session.execute(statementWithBindings).all()
    selected.get(0).getUUID("last_transaction")
  }
}
