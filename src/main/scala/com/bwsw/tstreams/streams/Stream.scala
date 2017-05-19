package com.bwsw.tstreams.streams

import com.bwsw.tstreams.env.defaults.TStreamsFactoryStreamDefaults
import com.bwsw.tstreams.storage.StorageClient
import org.apache.curator.framework.CuratorFramework

/**
  * @param client          Client to storage
  * @param id              stream identifier
  * @param name            Name of the stream
  * @param partitionsCount Number of stream partitions
  * @param ttl             Time of transaction time expiration in seconds
  * @param description     Some additional info about stream
  */
class Stream(val client: StorageClient,
             val curator: CuratorFramework,
             val id: Int,
             val name: String,
             val partitionsCount: Int,
             val ttl: Long,
             val description: String) {
  if (ttl < TStreamsFactoryStreamDefaults.Stream.ttlSec.min)
    throw new IllegalArgumentException(s"The TTL must be greater or equal than ${TStreamsFactoryStreamDefaults.Stream.ttlSec.min} seconds.")

  def shutdown() = {
    client.shutdown()
  }
}
