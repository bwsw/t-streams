package com.bwsw.tstreams.data.cassandra

import java.nio.ByteBuffer
import java.util

import com.bwsw.tstreams.data.IStorage
import com.datastax.driver.core._
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

/**
  * Cassandra storage impl of IStorage
  */
class Storage(cluster: Cluster, session: Session, keyspace: String) extends IStorage[Array[Byte]] {

  /**
    * CassandraStorage logger for logging
    */
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Prepared C* statement for data insertion
    */
  private val insertStatement = session
    .prepare(s"INSERT INTO $keyspace.data_queue (stream,partition,transaction,seq,data) values(?,?,?,?,?) USING TTL ?")

  /**
    * Prepared C* statement for select queries
    */
  private val selectStatement = session
    .prepare(s"SELECT data FROM $keyspace.data_queue WHERE stream=? AND partition=? AND transaction=? AND seq>=? AND seq<=? LIMIT ?")

  /**
    * Get data from cassandra storage
    *
    * @param streamName  Name of the stream
    * @param partition   Number of stream partitions
    * @param transaction Number of stream transactions
    * @param from        Data unique number from which reading will start
    * @param to          Data unique number from which reading will stop
    * @return Queue of object which have storage type
    */
  override def get(streamName: String,
                   partition: Int,
                   transaction: Long,
                   from: Int,
                   to: Int): scala.collection.mutable.Queue[Array[Byte]] = {
    val values: List[AnyRef] = List(streamName, new Integer(partition), new java.lang.Long(transaction), new Integer(from), new Integer(to), new Integer(to - from + 1))

    val statementWithBindings = selectStatement.bind(values: _*)

    val selected: util.List[Row] = session.execute(statementWithBindings).all()

    val it = selected.iterator()
    val data = scala.collection.mutable.Queue[Array[Byte]]()

    while (it.hasNext) {
      val obj = it.next().getObject("data").asInstanceOf[ByteBuffer].array()
      data.enqueue(obj)
    }

    data
  }

  /**
    * Checking closed or not this storage
    *
    * @return Closed concrete storage or not
    */
  override def isClosed(): Boolean = session.isClosed

  override def save(transaction: Long,
                    stream: String,
                    partition: Int,
                    ttl: Int,
                    lastItm: Int,
                    data: ListBuffer[Array[Byte]]): () => Unit = {

    val batchStatement = new BatchStatement()
    var i = lastItm - data.size

    data foreach { x => {
      val statementWithBindings = insertStatement.bind(
        stream,
        new Integer(partition),
        new java.lang.Long(transaction),
        new Integer(i),
        ByteBuffer.wrap(x),
        new Integer(ttl))
      batchStatement.add(statementWithBindings)
      i += 1
    }
    }
    session.execute(batchStatement)
    null
  }
}
