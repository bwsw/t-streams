package com.bwsw.tstreams.agents.producer

import com.bwsw.tstreams.common.StorageClient

import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.duration._


/**
  * Created by Ivan Kudryavtsev on 15.08.16.
  */
class ProducerTransactionData(transaction: ProducerTransaction, ttl: Long, storageClient: StorageClient) {
  private[tstreams] var items = ListBuffer[Array[Byte]]()
  private[tstreams] var lastOffset: Int = 0

  private val streamName = transaction.getTransactionOwner().stream.name

  def put(elt: Array[Byte]): Int = this.synchronized {
    items += elt
    return items.size
  }

  def save(): () => Unit = this.synchronized {
    val job = storageClient.client.putTransactionData(streamName, transaction.getPartition, transaction.getTransactionID(), items, lastOffset)

    if (Producer.logger.isDebugEnabled()) {
      Producer.logger.debug(s"putTransactionData($streamName, ${transaction.getPartition}, ${transaction.getTransactionID()}, $items, $lastOffset)")
    }

    lastOffset += items.size
    items = ListBuffer[Array[Byte]]()
    () => Await.result(job, 1.minute)
  }
}
