package com.bwsw.tstreams.data.hazelcast

import java.util.concurrent.atomic.AtomicBoolean
import com.hazelcast.config.XmlConfigBuilder
import com.hazelcast.core.Hazelcast
import com.hazelcast.core.HazelcastInstance
import java.util.{UUID, Map}

/**
  * Created by ivan on 12.08.16.
  */
class Factory {
  def getInstance(opts: Options): Storage = {
    new Storage(Factory.init(opts).getReplicatedMap(opts.keyspace))
  }
}

object Factory {
  val isInitialized = new AtomicBoolean(false)
  var hazelcastInstance: HazelcastInstance = null
  def init(opts: Options): HazelcastInstance = {
    if(isInitialized.getAndSet(true))
      return hazelcastInstance

    val config = new XmlConfigBuilder().build()
    config.getMapConfig(opts.keyspace)
      .setBackupCount(opts.synchronousReplicas)
      .setAsyncBackupCount(opts.asynchronousReplicas)

    hazelcastInstance = Hazelcast.newHazelcastInstance(config)
    hazelcastInstance
  }

  type StorageMapType = Map[String, List[Array[Byte]]]
}