package com.bwsw.tstreams.streams

import com.bwsw.tstreams.common.StorageClient

/**
  * Settings of stream in metadata storage
  *
  * @param name        Stream name
  * @param partitions  Number of stream partitions
  * @param ttl         Time in seconds of transaction expiration
  * @param description Some stream additional info
  */
case class StreamSettings(name: String, partitions: Int, ttl: Int, description: String)

object Stream {

  /**
    * Retrieving stream with concrete name
    *
    * @param name Stream name to fetch from database
    * @return StreamSettings
    */
  def getStream(storageClient: StorageClient, name: String): Option[StreamSettings] = ???
  /**
    * Create stream with parameters
    *
    * @param name        Stream name to use (unique id)
    * @param partitions  Number of stream partitions
    * @param ttl         Amount of expiration time of transaction
    * @param description Stream arbitrary description and com.bwsw.tstreams.metadata, etc.
    * @return StreamSettings
    */
  def createStream(storageClient: StorageClient,
                   name: String,
                   partitions: Int,
                   ttl: Int,
                   description: String): Unit = ???

  /**
    * Alternate stream with parameters
    *
    * @param name        Stream name to use (unique id)
    * @param partitions  Number of stream partitions
    * @param ttl         Amount of expiration time of transaction
    * @param description Stream arbitrary description and com.bwsw.tstreams.metadata, etc.
    * @return StreamSettings
    */
  def changeStream(storageClient: StorageClient, name: String, partitions: Int, ttl: Int, description: String): Unit = ???

  /**
    * Deleting concrete stream
    *
    * @param name Stream name to delete
    */
  def deleteStream(storageClient: StorageClient, name: String): Unit = ???

  /**
    * Checking that concrete stream exist
    *
    * @param name Stream name to check if exists
    * @return Exist stream or not
    */
  def isExist(storageClient: StorageClient, name: String): Boolean = ???
}

/**
  * @param name            Name of the stream
  * @param partitionsCount Number of stream partitions
  * @param storageClient   Client to storage
  * @param ttl             Time of transaction time expiration in seconds
  * @param description     Some additional info about stream
  * @tparam T Storage data type
  */
class Stream[T](val name: String,
                var partitionsCount: Int,
                val storageClient: StorageClient,
                var ttl: Int,
                var description: String) {
  /**
    * Transaction minimum ttl time
    */
  private val minTransactionTTL = 3

  if (ttl < minTransactionTTL)
    throw new IllegalArgumentException(s"The TTL must be greater or equal than $minTransactionTTL seconds.")

  /**
    * Save stream info in metadata
    */
  def save(): Unit = {
    Stream.changeStream(storageClient, name, partitionsCount, ttl, description)
  }

}
