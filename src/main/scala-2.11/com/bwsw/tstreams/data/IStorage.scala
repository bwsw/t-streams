package com.bwsw.tstreams.data

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable.ListBuffer

/**
  * Interface of data storage
  *
  * @tparam T Storage data type
  */
trait IStorage[T] {

  /**
    * Flag indicating binded this IStorage or not
    */
  private val isBound = new AtomicBoolean(false)


  /**
    * @return Closed concrete storage or not
    */
  def isClosed(): Boolean

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
    * Bind this storage for agent
    */
  def bind() = {
    if (isBound.getAndSet(true))
      throw new IllegalStateException("Storage instance is bound already.")
  }


  /**
    * Saves data to storage
    */
  def save(txn: UUID, stream: String, partition: Int, ttl: Int, lastItm: Int, data: ListBuffer[Array[Byte]]): () => Unit
}