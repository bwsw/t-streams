package com.bwsw.tstreams.data.aerospike

import java.util.UUID

import com.aerospike.client._
import com.bwsw.tstreams.data.IStorage
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

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


  override def save(txn: UUID,
                    stream: String,
                    partition: Int,
                    ttl: Int,
                    lastItm: Int,
                    data: ListBuffer[Array[Byte]]): () => Unit = {

    val key: Key = new Key(options.namespace, s"${stream}/${partition}", txn.toString)
    if(data.size == 0)
      return null
    var i = lastItm - data.size
    val mapped = data map {
      el => {
        val b = new Bin(i.toString, el)
        i+=1
        b
      }
    }

    client.put(options.writePolicy, key, mapped: _*)
    null
  }
}