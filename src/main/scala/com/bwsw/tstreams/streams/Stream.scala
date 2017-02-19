package com.bwsw.tstreams.streams

import com.bwsw.tstreams.common.StorageClient
import com.bwsw.tstreams.env.defaults.TStreamsFactoryStreamDefaults

import scala.concurrent.Await
import scala.concurrent.duration._

object Stream {

  var OP_TIMEOUT = 1.minute

  /**
    * Retrieving stream with concrete name
    *
    * @param name Stream name to fetch from database
    * @return StreamSettings
    */
  def getStream(storageClient: StorageClient, name: String): Stream = {
    val rpcStream = Await.result(storageClient.client.getStream(name), OP_TIMEOUT)
    new Stream(storageClient = storageClient, name = rpcStream.name, partitionsCount = rpcStream.partitions,
      ttl = rpcStream.ttl, description = rpcStream.description.fold("")(x => x))
  }

  /**
    * Create stream with parameters
    *
    * @param name        Stream name to use (unique id)
    * @param partitions  Number of stream partitions
    * @param ttl         Amount of expiration time of transaction
    * @param description Stream arbitrary description and com.bwsw.tstreams.metadata, etc.
    * @return StreamSettings
    */
  def createStream(storageClient: StorageClient, name: String, partitions: Int, ttl: Long, description: String): Boolean = {
    //TODO: fixit Long -> Int (TTL)
    Await.result(storageClient.client.putStream(name, partitions, Some(description), ttl.toInt), OP_TIMEOUT)
  }

  /**
    * Deleting concrete stream
    *
    * @param name Stream name to delete
    */
  def deleteStream(storageClient: StorageClient, name: String): Unit = {
    Await.result(storageClient.client.delStream(name), OP_TIMEOUT)
  }

  /**
    * Checking that concrete stream exist
    *
    * @param name Stream name to check if exists
    * @return Exist stream or not
    */
  def checkExists(storageClient: StorageClient, name: String): Boolean = {
    Await.result(storageClient.client.doesStreamExist(name), OP_TIMEOUT)
  }

}

/**
  * @param storageClient   Client to storage
  * @param name            Name of the stream
  * @param partitionsCount Number of stream partitions
  * @param ttl             Time of transaction time expiration in seconds
  * @param description     Some additional info about stream
  */
class Stream(val storageClient: StorageClient, val name: String, val partitionsCount: Int, val ttl: Long, val description: String) {


  if (ttl < TStreamsFactoryStreamDefaults.Stream.ttlSec.min)
    throw new IllegalArgumentException(s"The TTL must be greater or equal than ${TStreamsFactoryStreamDefaults.Stream.ttlSec.min} seconds.")
}
