/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.bwsw.tstreams.env

import java.security.InvalidParameterException
import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreams.agents.consumer.Offset.IOffset
import com.bwsw.tstreams.agents.consumer.subscriber.{QueueBuilder, Subscriber, SubscriberOptionsBuilder}
import com.bwsw.tstreams.agents.consumer.{Consumer, ConsumerOptions}
import com.bwsw.tstreams.agents.group.CheckpointGroup
import com.bwsw.tstreams.agents.producer.{Producer, ProducerOptions}
import com.bwsw.tstreams.common.{RoundRobinPartitionIterationPolicy, _}
import com.bwsw.tstreams.env.defaults.TStreamsFactoryProducerDefaults.PortRange
import com.bwsw.tstreams.env.defaults.TStreamsFactoryStorageClientDefaults
import com.bwsw.tstreams.storage.StorageClient
import com.bwsw.tstreams.streams.Stream
import com.bwsw.tstreamstransactionserver.options.ClientOptions.{AuthOptions, ConnectionOptions}
import com.bwsw.tstreamstransactionserver.options.CommonOptions.{TracingOptions, ZookeeperOptions}
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

/**
  * Created by Ivan Kudryavtsev on 21.07.16.
  */
class TStreamsFactory() {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val propertyMap = mutable.HashMap[String, Any]()
  private val storageClientList = mutable.ListBuffer[StorageClient]()

  val isClosed = new AtomicBoolean(false)
  val isLocked = new AtomicBoolean(false)
  val co = ConfigurationOptions

  propertyMap ++= defaults.TStreamsFactoryCommonDefaults.get
  propertyMap ++= defaults.TStreamsFactoryStorageClientDefaults.get
  propertyMap ++= defaults.TStreamsFactoryStreamDefaults.get
  propertyMap ++= defaults.TStreamsFactoryCoordinationDefaults.get
  propertyMap ++= defaults.TStreamsFactoryProducerDefaults.get
  propertyMap ++= defaults.TStreamsFactoryConsumerDefaults.get

  /**
    * locks factory, after lock setProperty leads to exception.
    */
  def lock(): Unit = isLocked.set(true)

  /**
    * clones factory
    */
  def copy(): TStreamsFactory = this.synchronized {
    if (isClosed.get)
      throw new IllegalStateException("TStreamsFactory is closed. This is the illegal usage of the object.")

    val f = new TStreamsFactory()
    propertyMap.foreach((kv) => f.setProperty(kv._1, kv._2))
    f
  }

  /**
    *
    * @param key
    * @param value
    * @return
    */
  def setProperty(key: String, value: Any): TStreamsFactory = this.synchronized {
    if (isClosed.get)
      throw new IllegalStateException("TStreamsFactory is closed. This is the illegal usage of the object.")

    if (isLocked.get)
      throw new IllegalStateException("TStreamsFactory is locked. Use clone() to set properties.")

    logger.debug("set property " + key + " = " + value)
    if (propertyMap contains key)
      propertyMap += (key -> value)
    else
      throw new IllegalArgumentException("Property " + key + " is unknown and can not be altered.")
    this
  }

  /**
    *
    * @param key
    * @return
    */
  def getProperty(key: String): Any = this.synchronized {
    if (isClosed.get)
      throw new IllegalStateException("TStreamsFactory is closed. This is the illegal usage of the object.")

    val v = propertyMap get key
    logger.debug("get property " + key + " = " + v.orNull)
    v.orNull
  }

  /** variant method to get option as int with default value if null
    *
    * @param key     key to request
    * @param default assign it if the value received from options is null
    * @return
    */
  private def pAsInt(key: String, default: Int = 0): Int = if (null == getProperty(key)) default else getProperty(key).toString.toInt

  /**
    * variant method to get option as string with default value if null
    *
    * @param key     key to request
    * @param default assign it if the value received from options is null
    * @return
    */
  private def pAsString(key: String, default: String = null): String = {
    val s = getProperty(key)
    if (null == s)
      return default
    s.toString
  }

  def getStorageClient(): StorageClient = this.synchronized {

    val clientOptions = ConnectionOptions(
      connectionTimeoutMs = pAsInt(co.StorageClient.connectionTimeoutMs),
      requestTimeoutMs = pAsInt(co.StorageClient.requestTimeoutMs),
      retryDelayMs = pAsInt(co.StorageClient.retryDelayMs),
      threadPool = pAsInt(co.StorageClient.threadPool),
      prefix = pAsString(co.Coordination.path))

    val authOptions = AuthOptions(key = pAsString(co.Common.authenticationKey))

    val zookeeperOptions = ZookeeperOptions(endpoints = pAsString(co.Coordination.endpoints))

    val curator = CuratorFrameworkFactory.builder()
      .connectionTimeoutMs(pAsInt(co.Coordination.connectionTimeoutMs))
      .sessionTimeoutMs(pAsInt(co.Coordination.sessionTimeoutMs))
      .retryPolicy(new ExponentialBackoffRetry(pAsInt(co.Coordination.retryDelayMs),
        pAsInt(co.Coordination.retryCount)))
      .connectString(pAsString(co.Coordination.endpoints)).build()

    curator.start()

    val tracingOptions =
      if (propertyMap.get(co.StorageClient.tracingEnabled).exists(_.asInstanceOf[Boolean]))
        TracingOptions(
          enabled = true,
          endpoint = propertyMap
            .get(co.StorageClient.tracingAddress)
            .map(_.asInstanceOf[String])
            .getOrElse(TStreamsFactoryStorageClientDefaults.StorageClient.tracingAddress))
      else TracingOptions(enabled = false)

    val client = new StorageClient(
      clientOptions = clientOptions,
      authOptions = authOptions,
      zookeeperOptions = zookeeperOptions,
      curator = curator,
      tracingOptions = tracingOptions)

    if (logger.isDebugEnabled)
      storageClientList.append(client)

    client
  }

  def getCheckpointGroup(executors: Int = 1) = new CheckpointGroup(executors)

  /**
    * Special debugging method to find leaks in factory. Used only in tests.
    * To accumulate requested storage client allocations DEBUG must be enabled.
    **/
  private[tstreams] def dumpStorageClients() = {
    storageClientList.foreach(clt => {
      logger.debug(s"$clt -> ${clt.isShutdown}")
    })
  }

  /**
    * common routine which allows to get ready to use stream object by env
    *
    * @return
    */
  private def getStream() = this.synchronized {
    val streamDefaults = defaults.TStreamsFactoryStreamDefaults

    if (isClosed.get)
      throw new IllegalStateException("TStreamsFactory is closed. This is the illegal usage of the object.")

    assert(pAsString(co.Stream.name) != null)
    streamDefaults.Stream.partitionsCount.check(pAsInt(co.Stream.partitionsCount, streamDefaults.Stream.partitionsCount.default))
    streamDefaults.Stream.ttlSec.check(pAsInt(co.Stream.ttlSec, streamDefaults.Stream.ttlSec.default))

    getStorageClient().loadStream(pAsString(co.Stream.name))
  }

  /**
    * reusable method which returns consumer options object
    */
  private def getBasicConsumerOptions(stream: Stream,
                                      partitions: Set[Int],
                                      offset: IOffset,
                                      checkpointAtStart: Boolean = false,
                                      useLastOffset: Boolean = true) = this.synchronized {

    val consumerDefaults = defaults.TStreamsFactoryConsumerDefaults

    val consumerTransactionsPreload = pAsInt(co.Consumer.transactionPreload, consumerDefaults.Consumer.transactionPreload.default)
    consumerDefaults.Consumer.transactionPreload.check(consumerTransactionsPreload)

    val consumerDataPreload = pAsInt(co.Consumer.dataPreload, consumerDefaults.Consumer.dataPreload.default)
    consumerDefaults.Consumer.dataPreload.check(consumerDataPreload)

    val consumerOptions = ConsumerOptions(
      transactionsPreload = consumerTransactionsPreload,
      dataPreload = consumerDataPreload,
      readPolicy = new RoundRobinPartitionIterationPolicy(stream.partitionsCount, partitions), offset = offset,
      useLastOffset = useLastOffset,
      checkpointAtStart = checkpointAtStart)

    consumerOptions
  }

  /**
    * returns ready to use producer object
    *
    * @param name Producer name
    * @param partitions
    * @return
    */
  def getProducer(name: String,
                  partitions: Set[Int]
                 ): Producer = this.synchronized {

    if (isClosed.get)
      throw new IllegalStateException("TStreamsFactory is closed. This is the illegal usage of the object.")


    val stream = getStream()

    val coordinationDefaults = defaults.TStreamsFactoryCoordinationDefaults.Coordination
    val producerDefaults = defaults.TStreamsFactoryProducerDefaults.Producer

    val zkSessionTimeoutMs = pAsInt(co.Coordination.sessionTimeoutMs, coordinationDefaults.sessionTimeoutMs.default)
    coordinationDefaults.sessionTimeoutMs.check(zkSessionTimeoutMs)

    val zkConnectionTimeoutMs = pAsInt(co.Coordination.connectionTimeoutMs, coordinationDefaults.connectionTimeoutMs.default)
    coordinationDefaults.connectionTimeoutMs.check(zkConnectionTimeoutMs)

    val notifyJobsThreadPoolSize = pAsInt(co.Producer.notifyJobsThreadPoolSize, producerDefaults.notifyJobsThreadPoolSize.default)
    producerDefaults.notifyJobsThreadPoolSize.check(notifyJobsThreadPoolSize)

    val transactionTtlMs = pAsInt(co.Producer.Transaction.ttlMs, producerDefaults.Transaction.ttlMs.default)
    producerDefaults.Transaction.ttlMs.check(transactionTtlMs)

    val transactionKeepAliveMs = pAsInt(co.Producer.Transaction.keepAliveMs, producerDefaults.Transaction.keepAliveMs.default)
    producerDefaults.Transaction.keepAliveMs.check(transactionKeepAliveMs)

    val batchSize = pAsInt(co.Producer.Transaction.batchSize, producerDefaults.Transaction.batchSize.default)
    producerDefaults.Transaction.batchSize.check(batchSize)

    var writePolicy: PartitionIterationPolicy = null

    if (pAsString(co.Producer.Transaction.distributionPolicy) ==
      co.Producer.Transaction.Constants.DISTRIBUTION_POLICY_RR) {
      writePolicy = new RoundRobinPartitionIterationPolicy(stream.partitionsCount, partitions)
    }
    else {
      throw new InvalidParameterException("Only TSF_Dictionary.Producer.Transaction.Consts.DISTRIBUTION_POLICY_RR policy " +
        "is supported currently in UniversalFactory.")
    }

    assert(transactionTtlMs >= transactionKeepAliveMs * 3)


    val po = new ProducerOptions(
      transactionTtlMs = transactionTtlMs,
      transactionKeepAliveMs = transactionKeepAliveMs,
      writePolicy = writePolicy,
      batchSize = batchSize,
      notifyJobsThreadPoolSize = notifyJobsThreadPoolSize)

    Try(new Producer(name = name, stream = stream, producerOptions = po)) match {
      case Success(producer) => producer
      case Failure(exception) =>
        stream.shutdown()
        throw exception
    }
  }

  /**
    * returns ready to use consumer object
    *
    * @param name Consumer name
    * @param partitions
    * @return
    */
  def getConsumer(name: String,
                  partitions: Set[Int],
                  offset: IOffset,
                  useLastOffset: Boolean = true,
                  checkpointAtStart: Boolean = false): Consumer = this.synchronized {

    if (isClosed.get)
      throw new IllegalStateException("TStreamsFactory is closed. This is the illegal usage of the object.")


    val stream = getStream()
    val consumerOptions = getBasicConsumerOptions(
      stream = stream, partitions = partitions,
      offset = offset, checkpointAtStart = checkpointAtStart,
      useLastOffset = useLastOffset)

    Try(new Consumer(name, stream, consumerOptions)) match {
      case Success(consumer) => consumer
      case Failure(exception) =>
        stream.shutdown()
        throw exception
    }
  }


  /**
    * returns ready to use subscribing consumer object
    *
    * @param partitions
    * @param callback
    * @return
    */
  def getSubscriber(name: String,
                    partitions: Set[Int],
                    callback: com.bwsw.tstreams.agents.consumer.subscriber.Callback,
                    offset: IOffset,
                    useLastOffset: Boolean = true,
                    checkpointAtStart: Boolean = false): Subscriber = this.synchronized {
    if (isClosed.get)
      throw new IllegalStateException("TStreamsFactory is closed. This is the illegal usage of the object.")

    val coordinationDefaults = defaults.TStreamsFactoryCoordinationDefaults.Coordination
    val consumerDefaults = defaults.TStreamsFactoryConsumerDefaults

    val stream = getStream()

    val consumerOptions = getBasicConsumerOptions(
      stream = stream,
      partitions = partitions,
      checkpointAtStart = checkpointAtStart,
      offset = offset,
      useLastOffset = useLastOffset)

    val bind_host = pAsString(co.Consumer.Subscriber.bindHost)
    assert(bind_host != null)
    assert(co.Consumer.Subscriber.bindPort != null)

    val bind_port = getProperty(co.Consumer.Subscriber.bindPort) match {
      case (p: Int) => p
      case PortRange(pFrom: Int, pTo: Int) => SpareServerSocketLookupUtility.findSparePort(bind_host, pFrom, pTo).get
    }

    val endpoints = pAsString(co.Coordination.endpoints)
    assert(endpoints != null)

    val root = pAsString(co.Coordination.path)
    assert(root != null)

    val sessionTimeoutMs = pAsInt(co.Coordination.sessionTimeoutMs, coordinationDefaults.sessionTimeoutMs.default)
    coordinationDefaults.sessionTimeoutMs.check(sessionTimeoutMs)

    val connectionTimeoutMs = pAsInt(co.Coordination.connectionTimeoutMs, coordinationDefaults.connectionTimeoutMs.default)
    coordinationDefaults.connectionTimeoutMs.check(connectionTimeoutMs)

    val transactionBufferThreadPoolSize = pAsInt(co.Consumer.Subscriber.transactionBufferThreadPoolSize, consumerDefaults.Consumer.Subscriber.transactionBufferThreadPoolSize.default)
    consumerDefaults.Consumer.Subscriber.transactionBufferThreadPoolSize.check(transactionBufferThreadPoolSize)

    val processingEnginesThreadPoolSize = pAsInt(co.Consumer.Subscriber.processingEnginesThreadPoolSize, consumerDefaults.Consumer.Subscriber.processingEnginesThreadPoolSize.default)
    consumerDefaults.Consumer.Subscriber.processingEnginesThreadPoolSize.check(processingEnginesThreadPoolSize)

    val pollingFrequencyDelayMs = pAsInt(co.Consumer.Subscriber.pollingFrequencyDelayMs, consumerDefaults.Consumer.Subscriber.pollingFrequencyDelayMs.default)
    consumerDefaults.Consumer.Subscriber.pollingFrequencyDelayMs.check(pollingFrequencyDelayMs)

    val transactionQueueMaxLengthThreshold = pAsInt(co.Consumer.Subscriber.transactionQueueMaxLengthThreshold, consumerDefaults.Consumer.Subscriber.transactionQueueMaxLengthThreshold.default)
    consumerDefaults.Consumer.Subscriber.transactionQueueMaxLengthThreshold.check(transactionQueueMaxLengthThreshold)

    val opts = SubscriberOptionsBuilder.fromConsumerOptions(consumerOptions,
      agentAddress = bind_host + ":" + bind_port,
      zkPrefixPath = root,
      transactionsBufferWorkersThreadPoolSize = transactionBufferThreadPoolSize,
      processingEngineWorkersThreadSize = processingEnginesThreadPoolSize,
      pollingFrequencyDelayMs = pollingFrequencyDelayMs,
      transactionQueueMaxLengthThreshold = transactionQueueMaxLengthThreshold,
      transactionsQueueBuilder = new QueueBuilder.InMemory())


    Try(new Subscriber(name, stream, opts, callback)) match {
      case Success(subscriber) => subscriber
      case Failure(exception) =>
        stream.shutdown()
        throw exception
    }
  }

  /**
    * closes t-streams factory and stops further object creation
    */
  def close(): Unit = {
    if (isClosed.getAndSet(true))
      throw new IllegalStateException("TStreamsFactory is closed. This is repeatable close operation.")
  }

}
