package com.bwsw.tstreams.data.aerospike

import java.util.UUID

import com.aerospike.client._
import com.bwsw.tstreams.data.IStorage
import org.slf4j.LoggerFactory

import scala.collection.mutable
import com.bwsw.tstreams.common.FirstFailLockableTaskExecutor

/**
  * Aerospike storage impl of IStorage
  *
  * @param options User defined aerospike options
  * @param client  Aerospike client instance
  */
class Storage(client: AerospikeClient, options: Options) extends IStorage[Array[Byte]] {

  /**
    * AerospikeStorage logger for logging
    */
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * @return Closed concrete storage or not
    */
  override def isClosed(): Boolean =
    client.isConnected

  /**
    * Initialize data storage
    */
  override def init(): Unit = {
    logger.warn("Aerospike data storage doesn't require initialization")
  }

  /**
    * Get data from storage
    *
    * @param streamName  Name of the stream
    * @param partition   Number of stream partitions
    * @param transaction Number of stream transactions
    * @param from        Data unique number from which reading will start
    * @param to          Data unique number from which reading will stop
    * @return Queue of object which have storage type
    */
  override def get(streamName: String, partition: Int, transaction: UUID, from: Int, to: Int): mutable.Queue[Array[Byte]] = {
    val key: Key = new Key(options.namespace, s"$streamName/$partition", transaction.toString)
    val names = (from to to).toList.map(x => x.toString)
    //    logger.debug(s"Start retrieving data from aerospike for streamName: {$streamName}, partition: {$partition}")
    val record: Record = client.get(options.readPolicy, key, names: _*)
    //    logger.debug(s"Finished retrieving data from aerospike for streamName: {$streamName}, partition: {$partition}")

    val data = scala.collection.mutable.Queue[Array[Byte]]()
    for (name <- names) {
      data.enqueue(record.getValue(name).asInstanceOf[Array[Byte]])
    }
    data
  }

  /**
    * Put data in storage
    *
    * @param streamName  Name of the stream
    * @param partition   Number of stream partitions
    * @param transaction Number of stream transactions
    * @param data        Data which will be put
    * @param partNum     Data unique number
    * @param ttl         Time of records expiration in seconds
    * @return Null instead of wait lambda because client.put is not async
    */
  override def put(streamName: String, partition: Int, transaction: UUID, ttl: Int, data: Array[Byte], partNum: Int): () => Unit = {
    options.writePolicy.expiration = ttl
    val key: Key = new Key(options.namespace, s"$streamName/$partition", transaction.toString)
    val bin = new Bin(partNum.toString, data)
    client.put(options.writePolicy, key, bin)
    null
  }

  /**
    * Remove all data in data storage
    */
  override def truncate(): Unit = {
    logger.warn("aerospike can't be truncated")
  }

  /**
    * Remove storage
    */
  override def remove(): Unit = {
    logger.warn("aerospike data storage can't be removed")
  }

  /**
    * Save all info from buffer in IStorage
    *
    * @return Lambda which indicate done or not putting request(if request was async) null else
    */
  override def saveBuffer(txn: UUID): () => Unit = {
    if (buffer.contains(txn)) {
      val elem = buffer(txn).head
      options.writePolicy.expiration = elem.ttl
      val key: Key = new Key(options.namespace, s"${elem.streamName}/${elem.partition}", elem.transaction.toString)

      val mapped = buffer(txn) map { el => new Bin(el.partNum.toString, el.data) }
      if(logger.isDebugEnabled)
      {
        logger.debug(s"Start putting batch of data with size:${getBufferSize(txn)} in aerospike for streamName: {${elem.streamName}}, partition: {${elem.partition}")
      }

      client.put(options.writePolicy, key, mapped: _*)

      if(logger.isDebugEnabled)
      {
        logger.debug(s"Finished putting batch of data with size:${getBufferSize(txn)} in aerospike for streamName: {${elem.streamName}}, partition: {${elem.partition}")
      }
    }
    null
  }
}