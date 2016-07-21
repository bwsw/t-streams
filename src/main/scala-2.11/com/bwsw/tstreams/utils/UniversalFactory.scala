package com.bwsw.tstreams.utils

import com.bwsw.tstreams.agents.consumer.BasicConsumer
import com.bwsw.tstreams.agents.consumer.subscriber.{BasicSubscriberCallback, BasicSubscribingConsumer}
import com.bwsw.tstreams.agents.producer.BasicProducer
import com.bwsw.tstreams.converter.IConverter
import com.bwsw.tstreams.generator.IUUIDGenerator
import com.datastax.driver.core.policies.RoundRobinPolicy

import scala.collection.mutable.{Map, HashMap}

/**
  * Created by ivan on 21.07.16.
  */
class UniversalFactory {
  var propertyMap = new HashMap[String,Any]()
  propertyMap += ("actor-system.name" -> "T-Streams")
  propertyMap += ("metadata.cluster.endpoints" -> "localhost:9042")
  propertyMap += ("metadata.cluster.login" -> null)
  propertyMap += ("metadata.cluster.password" -> null)

  propertyMap += ("data.cluster.driver" -> "aerospike")
  propertyMap += ("data.cluster.endpoints" -> "localhost:3000")
  propertyMap += ("data.cluster.namespace" -> "test")
  propertyMap += ("data.cluster.login" -> null)
  propertyMap += ("data.cluster.password" -> null)

  propertyMap += ("coordination.endpoints" -> "localhost:2181")
  propertyMap += ("coordination.root" -> "/t-streams")
  propertyMap += ("coordination.ttl" -> 7)
  propertyMap += ("coordination.connection.timeout" -> 7)

  propertyMap += ("stream.name" -> "test")
  propertyMap += ("stream.partitions" -> 1)
  propertyMap += ("stream.ttl" -> 60 * 60 * 24)
  propertyMap += ("stream.description" -> "Test stream")

  propertyMap += ("producer.endpoint.host" -> "localhost")
  propertyMap += ("producer.endpoint.port" -> 18000)
  propertyMap += ("producer.transport.timeout" -> 5)
  propertyMap += ("producer.transaction.ttl" -> 6)
  propertyMap += ("producer.transaction.keepalive" -> 2)
  propertyMap += ("producer.transaction.insert-single" -> true)
  propertyMap += ("producer.transaction.insert-policy" -> RoundRobinPolicy)
  propertyMap += ("producer.thread-pool" -> 4)


  propertyMap += ("consumer.transaction.preload" -> 10)
  propertyMap += ("consumer.data.preload" -> 100)

  propertyMap += ("subscriber.endpoint.host" -> "localhost")
  propertyMap += ("subscriber.endpoint.port" -> 18001)
  propertyMap += ("subscriber.persistent-queue.path" -> "/tmp")


  def setProperty(key: String, value: Any): UniversalFactory = {
    if(propertyMap contains key)
      propertyMap += (key -> value)
    else
      throw new IllegalArgumentException("Property " + key + " is unknown and can not be altered.")
    return this
  }

  def getProperty(key: String): Any = propertyMap get key

  def getProducer[F,T](isLowPriority : Boolean,
                       txnGenerator : IUUIDGenerator,
                       converter : IConverter[F,T],
                       partitions : List
                      ): BasicProducer = {
    null
  }

  def getConsumer[F,T](txnGenerator : IUUIDGenerator,
                       converter : IConverter[F,T],
                       partitions : List
                      ): BasicConsumer = {
    null
  }

  def getSubscriber[F,T](txnGenerator : IUUIDGenerator,
                         converter : IConverter[F,T],
                         partitions : List,
                         callback: BasicSubscriberCallback): BasicSubscribingConsumer = {
    null
  }

}
