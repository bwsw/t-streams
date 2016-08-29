package com.bwsw.tstreams.data.hazelcast

import java.util.Map
import java.util.concurrent.atomic.AtomicBoolean

import com.hazelcast.config.XmlConfigBuilder
import com.hazelcast.core.{Hazelcast, HazelcastInstance}

/**
  * Created by Ivan Kudryavtsev on 12.08.16.
  */
class Factory {
  def getInstance(opts: Options): Storage = {
    new Storage(Factory.init(opts).getReplicatedMap(opts.keyspace))
  }
}

/**
  * Singleton which does all one-time initialization for Hazelcast
  */
object Factory {
  val isInitialized = new AtomicBoolean(false)
  var hazelcastInstance: HazelcastInstance = null

  /**
    * Initialization method itself. Works only one time, after returns created object
    * @param opts
    * @return
    */
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

  /**
    * Type stored in Hazelcast map
    */
  type StorageMapType = Map[String, List[Array[Byte]]]
}