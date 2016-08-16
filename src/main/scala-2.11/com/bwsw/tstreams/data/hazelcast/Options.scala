package com.bwsw.tstreams.data.hazelcast

/**
  * Created by Ivan Kudryavtsev on 12.08.16.
  * Hazelcast storage options
  * @param keyspace
  * @param synchronousReplicas
  * @param asynchronousReplicas
  */
case class Options(val keyspace: String, val synchronousReplicas: Int = 1, val asynchronousReplicas: Int = 0)
