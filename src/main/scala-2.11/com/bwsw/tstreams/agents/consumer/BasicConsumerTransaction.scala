package com.bwsw.tstreams.agents.consumer


import java.util.UUID

import com.bwsw.tstreams.entities.TransactionSettings

import scala.collection.mutable


/**
  * @param consumer    Consumer which this transaction was created by
  * @param partition   Partition for this transaction to consume
  * @param transaction Transaction time and total packets in it
  * @tparam USERTYPE User data type
  */
class BasicConsumerTransaction[USERTYPE](consumer: BasicConsumer[USERTYPE],
                                                   partition: Int,
                                                   transaction: TransactionSettings) {

  /**
    * Return transaction UUID
    */
  def getTxnUUID: UUID = transaction.txnUuid

  /**
    * Return transaction partition
    */
  def getPartition: Int = partition

  /**
    * Transaction data pointer
    */
  private var cnt = 0

  /**
    * Buffer to preload some amount of current transaction data
    */
  private var buffer: scala.collection.mutable.Queue[Array[Byte]] = null

  /**
    * @return Next piece of data from current transaction
    */
  def next(): USERTYPE = {
    if (!hasNext())
      throw new IllegalStateException("no data to consume")

    //try to update buffer
    if (buffer == null || buffer.isEmpty) {
      val newcnt = min2(cnt + consumer.options.dataPreload, transaction.totalItems - 1)
      buffer = consumer.stream.dataStorage.get(consumer.stream.getName, partition, transaction.txnUuid, cnt, newcnt)
      cnt = newcnt + 1
    }

    consumer.options.converter.convert(buffer.dequeue())
  }

  /**
    * Indicate consumed or not current transaction
    *
    * @return
    */
  def hasNext(): Boolean =
    cnt < transaction.totalItems || buffer.nonEmpty

  /**
    * Refresh BasicConsumerTransaction iterator to read from the beginning
    */
  def replay(): Unit = {
    buffer.clear()
    cnt = 0
  }

  /**
    * @return All consumed transaction
    */
  def getAll(): List[USERTYPE] = {
    val data: mutable.Queue[Array[Byte]] = consumer.stream.dataStorage.get(consumer.stream.getName, partition, transaction.txnUuid, cnt, transaction.totalItems - 1)
    data.toList.map(x => consumer.options.converter.convert(x))
  }

  /**
    * Helper function to find min value
    *
    * @param a First value
    * @param b Second value
    * @return Min value
    */
  private def min2(a: Int, b: Int): Int = if (a < b) a else b
}
