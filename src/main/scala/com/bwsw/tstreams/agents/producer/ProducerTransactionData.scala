package com.bwsw.tstreams.agents.producer

import com.bwsw.tstreams.common.StorageClient

import scala.concurrent.Await
import scala.concurrent.duration._


/**
  * Created by Ivan Kudryavtsev on 15.08.16.
  */
class ProducerTransactionData(transaction: ProducerTransaction, ttl: Long, storageClient: StorageClient) {
  private var items = new scala.collection.mutable.ListBuffer[Array[Byte]]()
  var lastOffset: Int = 0
  private val streamName = transaction.getTransactionOwner().stream.name

  def put(elt: Array[Byte]): Int = this.synchronized {
    items += elt
    lastOffset += 1
    return lastOffset
  }

  def save(): () => Unit = this.synchronized {
    val job = storageClient.client.putTransactionData(streamName, transaction.getPartition,
      transaction.getTransactionID(), items, lastOffset)

    items = new scala.collection.mutable.ListBuffer[Array[Byte]]()
    () => Await.result(job, 1.minute)
  }
}
