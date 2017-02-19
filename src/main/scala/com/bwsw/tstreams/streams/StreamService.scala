package com.bwsw.tstreams.streams

import com.bwsw.tstreams.common.StorageClient
import org.slf4j.LoggerFactory


/**
  * Service for streams
  */
object StreamService {

  /**
    * Basic Stream logger for logging
    */
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Getting existing stream
    *
    * @param storageClient   Storage of concrete stream
    * @param streamName      Name of the stream
    * @return Stream instance
    */
  def loadStream(storageClient: StorageClient, streamName: String): Stream = {
    val settings = Stream.getStream(storageClient, streamName)

    new Stream(storageClient = storageClient, name = settings.name, partitionsCount = settings.partitionsCount,
      ttl = settings.ttl, description = settings.description)
  }

  /**
    * Creating stream
    *
    * @param storageClient   Storage of concrete stream
    * @param streamName      Name of the stream
    * @param partitionsCount Number of stream partitions
    * @param description     Some additional info about stream
    * @param ttl             Expiration time of single transaction in seconds
    */
  def createStream(storageClient: StorageClient, streamName: String, partitionsCount: Int, ttl: Long, description: String): Stream = {

    if (Stream.createStream(storageClient, streamName, partitionsCount, ttl, description) == false)
      throw new IllegalArgumentException(s"Stream ${streamName} already exists.")
    new Stream(storageClient, streamName, partitionsCount, ttl, description)
  }


  /**
    * Deleting concrete stream
    *
    * @param storageClient   Storage of concrete stream
    * @param streamName      Name of the stream to delete
    */
  def deleteStream(storageClient: StorageClient, streamName: String): Unit = {
    Stream.deleteStream(storageClient, streamName)
  }


  /**
    * Checking exist concrete stream or not
    *
    * @param storageClient   Storage of concrete stream
    * @param streamName      Name of the stream to check
    */
  def checkExists(storageClient: StorageClient, streamName: String): Boolean =
    Stream.checkExists(storageClient, streamName)

}
