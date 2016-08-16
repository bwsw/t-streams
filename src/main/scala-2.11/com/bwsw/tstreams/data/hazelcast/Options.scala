package com.bwsw.tstreams.data.hazelcast

/**
  * Created by ivan on 12.08.16.
  */
case class Options(val keyspace: String, val synchronousReplicas: Int = 1, val asynchronousReplicas: Int = 0)
