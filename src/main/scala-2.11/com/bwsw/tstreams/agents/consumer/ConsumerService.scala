package com.bwsw.tstreams.agents.consumer

import com.bwsw.tstreams.metadata.RequestsRepository
import com.datastax.driver.core.{BatchStatement, Session}

/**
  * Consumer entity for interact with consumers metadata
  *
  * @param session    Session with metadata
  */
class ConsumerService(session: Session) {

  private val requests = RequestsRepository.getStatements(session)

  /**
    * Checking exist or not concrete consumer
    *
    * @param consumerName Name of the consumer
    * @return Exist or not concrete consumer
    */
  def offsetExists(consumerName: String, stream: String, partition: Int): Boolean = {
    val boundStatement = requests.consumerGetCheckpointStatement.bind(consumerName, stream, new Integer(partition))
    Option(session.execute(boundStatement).one()).isDefined
  }

  /**
    * Saving offset batch
    *
    * @param name                        Name of the consumer
    * @param stream                      Name of the specific stream
    * @param partitionAndLastTransaction Set of partition and last transaction pairs to save
    */
  def saveBatchOffset(name: String, stream: String, partitionAndLastTransaction: scala.collection.mutable.Map[Int, Long]): Unit = {
    val batchStatement = new BatchStatement()
    partitionAndLastTransaction.map { case (partition, lastTransaction) =>
      val values: List[AnyRef] = List(name, stream, new Integer(partition), new java.lang.Long(lastTransaction))
      val boundStatement = requests.consumerCheckpointStatement.bind(values: _*)
      batchStatement.add(boundStatement)
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
  def saveSingleOffset(name: String, stream: String, partition: Int, offset: Long): Unit = {
    val values: List[AnyRef] = List(name, stream, new Integer(partition), new java.lang.Long(offset))
    session.execute(requests.consumerCheckpointStatement.bind(values: _*))
  }

  /**
    * Retrieving specific offset for particular consumer
    *
    * @param name      Name of the specific consumer
    * @param stream    Name of the specific stream
    * @param partition Name of the specific partition
    * @return Offset
    */
  def getLastSavedOffset(name: String, stream: String, partition: Int): Long = {
    val values = List(name, stream, new Integer(partition))
    Option(session.execute(requests.consumerGetCheckpointStatement.bind(values: _*)).one())
      .fold(-1L)(row => row.getLong("last_transaction"))
  }
}
