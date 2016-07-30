package com.bwsw.tstreams.data

import java.util.UUID

/**
  * Interface of data storage
  *
  * @tparam T Storage data type
  */
trait IStorage[T] {

  /**
    * @return Closed concrete storage or not
    */
  def isClosed(): Boolean

  /**
    * Initialize data storage
    */
  def init()

  /**
    * Remove all data in data storage
    */
  def truncate()


  /**
    * Remove storage
    */
  def remove()

  /**
    * Put data in storage
    *
    * @param streamName  Name of the stream
    * @param partition   Number of stream partitions
    * @param transaction Number of stream transactions
    * @param data        Data which will be put
    * @param partNum     Data unique number
    * @param ttl         Time of records expiration in seconds
    * @return Lambda which indicate done or not putting request(if request was async) null else
    */
  def put(streamName: String, partition: Int, transaction: java.util.UUID, ttl: Int, data: T, partNum: Int): () => Unit

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
  def get(streamName: String, partition: Int, transaction: java.util.UUID, from: Int, to: Int): scala.collection.mutable.Queue[T]


  /**
    * Put data in buffer to save it later
    *
    * @param streamName  Name of the stream
    * @param partition   Number of stream partitions
    * @param transaction Number of stream transactions
    * @param data        Data which will be put
    * @param partNum     Data unique number
    * @param ttl         Time of records expiration in seconds
    */
  def putInBuffer(streamName: String, partition: Int, transaction: java.util.UUID, ttl: Int, data: T, partNum: Int): Unit = {
    if (!buffer.contains(transaction)) {
      buffer(transaction) = scala.collection.mutable.ListBuffer[dataToPush]()
    }
    buffer(transaction) += dataToPush(streamName, partition, transaction, ttl, data, partNum)
  }


  /**
    * Save all info from buffer in IStorage
    *
    * @return Lambda which indicate done or not putting request(if request was async) null else
    */
  def saveBuffer(txn: java.util.UUID): () => Unit


  /**
    * Clear current producer buffer
    */
  def clearBuffer(txn: java.util.UUID): Unit =
    if (buffer.contains(txn))
      buffer(txn).clear()


  /**
    * @return Buffer size
    */
  def getBufferSize(txn: java.util.UUID): Int = {
    if (!buffer.contains(txn)) 0 else buffer(txn).size
  }

  /**
    * Buffer for buffering data to push it with saveBuffer()
    */
  protected val buffer = scala.collection.mutable.Map[java.util.UUID, scala.collection.mutable.ListBuffer[dataToPush]]()

  /**
    * Helper class for buffering data
    *
    * @param streamName  Name of the stream to push data
    * @param partition   Number of the partition
    * @param transaction Number of the transaction
    * @param ttl         Ttl of how long data will exist
    * @param data        User data
    * @param partNum     Part number of data(in single txn can be >1 parts of userdata)
    */
  protected case class dataToPush(streamName: String, partition: Int, transaction: UUID, ttl: Int, data: T, partNum: Int)


  /**
    * Flag indicating binded this IStorage or not
    */
  private var isBound = false


  /**
    * Bind this storage for agent
    */
  def bind() = {
    if (isBound)
      throw new IllegalStateException("storage is already binded")
    isBound = true
  }
}