package com.bwsw.tstreams.data.hazelcast

import com.bwsw.tstreams.data.IStorage
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Created by Ivan Kudryavtsev on 12.08.16.
  */
class Storage(map: Factory.StorageMapType) extends IStorage[Array[Byte]] {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Get data from transaction from storage
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
                   to: Int): mutable.Queue[Array[Byte]] = synchronized {
    val key = s"$streamName-$partition-${transaction.toString}"
    val data = map.get(key).slice(from, to + 1)
    val rq = mutable.Queue[Array[Byte]]()
    data foreach (e => rq += e)
    rq
  }

  /**
    * Checking closed or not this storage
    *
    * @return Closed concrete storage or not
    */
  override def isClosed(): Boolean = false

  /**
    * Put data in transaction in storage
    *
    * @param transaction
    * @param stream
    * @param partition
    * @param ttl
    * @param lastItm
    * @param data
    * @return
    */
  override def save(transaction: Long,
                    stream: String,
                    partition: Int,
                    ttl: Int,
                    lastItm: Int,
                    data: mutable.ListBuffer[Array[Byte]]): () => Unit = synchronized {
    val key = s"$stream-$partition-${transaction.toString}"
    val dataFromMap = map.getOrDefault(key, List[Array[Byte]]())
    map.put(key, dataFromMap ++ data)
    null
  }
}
