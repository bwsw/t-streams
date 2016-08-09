package com.bwsw.tstreams.data.cassandra

import java.nio.ByteBuffer
import java.util
import java.util.UUID

import com.bwsw.tstreams.data.IStorage
import com.datastax.driver.core._
import org.slf4j.LoggerFactory


/**
  * Cassandra storage impl of IStorage
  */
class CassandraStorage(cluster: Cluster, session: Session, keyspace: String) extends IStorage[Array[Byte]] {

  /**
    * CassandraStorage logger for logging
    */
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Prepared C* statement for data insertion
    */
  private val insertStatement = session
    .prepare(s"INSERT INTO ${keyspace}.data_queue (stream,partition,transaction,seq,data) values(?,?,?,?,?) USING TTL ?")

  /**
    * Prepared C* statement for select queries
    */
  private val selectStatement = session
    .prepare(s"SELECT data FROM ${keyspace}.data_queue WHERE stream=? AND partition=? AND transaction=? AND seq>=? AND seq<=? LIMIT ?")

  /**
    * Put data in the cassandra storage
    *
    * @param streamName  Name of the stream
    * @param partition   Number of stream partitions
    * @param transaction Number of stream transactions
    * @param data        Data which will be put
    * @param partNum     Data unique part number
    * @return Wait lambda
    */
  override def put(streamName: String,
                   partition: Int,
                   transaction: UUID,
                   ttl: Int,
                   data: Array[Byte],
                   partNum: Int): () => Unit = {

    val values = List(streamName, new Integer(partition), transaction, new Integer(partNum), ByteBuffer.wrap(data), new Integer(ttl))

    val statementWithBindings = insertStatement.bind(values: _*)

    //    logger.debug(s"start inserting data for stream:{$streamName}, partition:{$partition}, partNum:{$partNum}\n")
    val res: ResultSetFuture = session
      .executeAsync(statementWithBindings)
    //    logger.debug(s"finished inserting data for stream:{$streamName}, partition:{$partition}, partNum:{$partNum}\n")

    val job: () => Unit = () => res.getUninterruptibly()
    job
  }

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
                   transaction: UUID,
                   from: Int,
                   to: Int): scala.collection.mutable.Queue[Array[Byte]] = {
    val values: List[AnyRef] = List(streamName, new Integer(partition), transaction, new Integer(from), new Integer(to), new Integer(to - from + 1))

    val statementWithBindings = selectStatement.bind(values: _*)

    //    logger.debug(s"start retrieving data for stream:{$streamName}, partition:{$partition}, from:{$from}, to:{$to}\n")
    val selected: util.List[Row] = session.execute(statementWithBindings).all()
    //    logger.debug(s"finished retrieving data for stream:{$streamName}, partition:{$partition}, from:{$from}, to:{$to}\n")

    val it = selected.iterator()
    val data = scala.collection.mutable.Queue[Array[Byte]]()

    while (it.hasNext) {
      val obj = it.next().getObject("data").asInstanceOf[ByteBuffer].array()
      data.enqueue(obj)
    }

    data
  }

  /**
    * Initialize data storage
    */
  override def init(): Unit = {
    logger.info("start initializing CassandraStorage table")

    session.execute(s"CREATE TABLE IF NOT EXISTS data_queue ( " +
      s"stream text, " +
      s"partition int, " +
      s"transaction timeuuid, " +
      s"seq int, " +
      s"data blob, " +
      s"PRIMARY KEY ((stream, partition), transaction, seq))")

    logger.info("finished initializing CassandraStorage table")
  }

  /**
    * Remove all data in data storage
    */
  override def truncate(): Unit = {
    logger.info("start truncating CassandraStorage data_queue table")

    session.execute("TRUNCATE data_queue")

    logger.info("finished truncating CassandraStorage data_queue table")
  }

  /**
    * Remove storage
    */
  override def remove(): Unit = {
    logger.info("start removing CassandraStorage data_queue table")

    session.execute("DROP TABLE IF EXISTS data_queue")

    logger.info("finished removing CassandraStorage data_queue table")
  }

  /**
    * Checking closed or not this storage
    *
    * @return Closed concrete storage or not
    */
  override def isClosed(): Boolean = session.isClosed && cluster.isClosed

  /**
    * Save all info from buffer in IStorage
    *
    * @return Lambda which indicate done or not putting request(if request was async) null else
    */
  override def saveBuffer(txn: UUID): () => Unit = {
    if (buffer.contains(txn)) {
      val batchStatement = new BatchStatement()
      buffer(txn) foreach { x =>
        val statementWithBindings = insertStatement.bind(
          x.streamName,
          new Integer(x.partition),
          x.transaction,
          new Integer(x.partNum),
          ByteBuffer.wrap(x.data),
          new Integer(x.ttl))

        batchStatement.add(statementWithBindings)

        logger.debug(s"Start putting batch of data with size:${getBufferSize(txn)} in cassandra for streamName: {${buffer(txn).head.streamName}}, partition: {${buffer(txn).head.streamName}")
        session.execute(batchStatement)
        logger.debug(s"Finished putting batch of data with size:${getBufferSize(txn)} in cassandra for streamName: {${buffer(txn).head.streamName}}, partition: {${buffer(txn).head.streamName}")
      }
    }
    null
  }
}
