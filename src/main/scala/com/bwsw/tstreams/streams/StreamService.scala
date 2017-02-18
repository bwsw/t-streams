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
    * @tparam T Type of stream data
    */
  def loadStream[T](storageClient: StorageClient, streamName: String): Stream[T] = {
    val settingsOpt: Option[StreamSettings] = Stream.getStream(storageClient, streamName)
    if (settingsOpt.isEmpty)
      throw new IllegalArgumentException("stream with this name can not be loaded")
    else {
      val settings = settingsOpt.get
      val (name: String, partitionsCount: Int, ttl: Int, description: String) =
        (settings.name, settings.partitionsCount, settings.ttl, settings.description)

      val stream: Stream = new Stream(storageClient = storageClient, name = name, partitionsCount = partitionsCount,
        ttl = ttl, description = description)
      stream
    }
  }

  /**
    * Creating stream
    *
    * @param storageClient   Storage of concrete stream
    * @param streamName      Name of the stream
    * @param partitions      Number of stream partitions
    * @param description     Some additional info about stream
    * @param ttl             Expiration time of single transaction in seconds
    * @tparam T Type of stream data
    */
  def createStream[T](storageClient: StorageClient,
                      streamName: String,
                      partitions: Int,
                      ttl: Int,
                      description: String): Stream = {

    Stream.createStream(storageClient, streamName, partitions, ttl, description)
    new Stream(storageClient, streamName, partitions, ttl, description)
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
  def doesExist(storageClient: StorageClient, streamName: String): Boolean =
    Stream.isExist(storageClient, streamName)

}
