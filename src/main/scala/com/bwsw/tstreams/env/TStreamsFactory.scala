package com.bwsw.tstreams.env

import java.security.InvalidParameterException
import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreams.agents.consumer.Offset.IOffset
import com.bwsw.tstreams.agents.consumer.subscriber.QueueBuilder.Persistent
import com.bwsw.tstreams.agents.consumer.subscriber.{QueueBuilder, Subscriber, SubscriberOptionsBuilder}
import com.bwsw.tstreams.agents.consumer.{Consumer, ConsumerOptions}
import com.bwsw.tstreams.agents.producer.{CoordinationOptions, Producer}
import com.bwsw.tstreams.common.{RoundRobinPolicy, _}
import com.bwsw.tstreams.coordination.client.TcpTransport
import com.bwsw.tstreams.env.defaults.TStreamsFactoryProducerDefaults.PortRange
import com.bwsw.tstreams.generator.{ITransactionGenerator, LocalTransactionGenerator}
import com.bwsw.tstreams.streams.Stream
import com.bwsw.tstreamstransactionserver.options.{AuthOptions, ClientOptions, ZookeeperOptions}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Created by Ivan Kudryavtsev on 21.07.16.
  */
class TStreamsFactory() {
  private val logger = LoggerFactory.getLogger(this.getClass)
  val propertyMap = mutable.HashMap[String, Any]()
  val isClosed = new AtomicBoolean(false)
  val isLocked = new AtomicBoolean(false)
  val co = ConfigurationOptions

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

  private def pAsLong(key: String, default: Long = 0L): Long = if (null == getProperty(key)) default else getProperty(key).toString.toLong


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

  def getStorageClient() = this.synchronized {
    val clientOptions = new ClientOptions(connectionTimeoutMs = pAsInt(co.StorageClient.connectionTimeoutMs),
      retryDelayMs = pAsInt(co.StorageClient.retryDelayMs),
      threadPool = pAsInt(co.StorageClient.threadPool))

    val authOptions = new AuthOptions(key = pAsString(co.StorageClient.Auth.key),
      connectionTimeoutMs = pAsInt(co.StorageClient.Auth.connectionTimeoutMs),
      retryDelayMs = pAsInt(co.StorageClient.Auth.retryDelayMs),
      tokenConnectionTimeoutMs = pAsInt(co.StorageClient.Auth.tokenConnectionTimeoutMs),
      tokenRetryDelayMs = pAsInt(co.StorageClient.Auth.tokenRetryDelayMs))

    val zookeeperOptions = new ZookeeperOptions(endpoints = pAsString(co.StorageClient.Zookeeper.endpoints),
      prefix = pAsString(co.StorageClient.Zookeeper.prefix),
      sessionTimeoutMs = pAsInt(co.StorageClient.Zookeeper.sessionTimeoutMs),
      connectionTimeoutMs = pAsInt(co.StorageClient.Zookeeper.connectionTimeoutMs),
      retryDelayMs = pAsInt(co.StorageClient.Zookeeper.retryDelayMs),
      retryCount = pAsInt(co.StorageClient.Zookeeper.retryCount))

    new StorageClient(clientOptions = clientOptions, authOptions = authOptions, zookeeperOptions = zookeeperOptions)
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



    // construct stream
    val stream = new Stream(
      storageClient = getStorageClient(),
      name = pAsString(co.Stream.name),
      partitionsCount = pAsInt(co.Stream.partitionsCount, streamDefaults.Stream.partitionsCount.default),
      ttl = pAsLong(co.Stream.ttlSec, streamDefaults.Stream.ttlSec.default),
      description = pAsString(co.Stream.description, ""))

    stream
  }

  /**
    * reusable method which returns consumer options object
    */
  private def getBasicConsumerOptions(stream: Stream,
                                      partitions: Set[Int],
                                      transactionGenerator: ITransactionGenerator,
                                      offset: IOffset,
                                      checkpointAtStart: Boolean = false,
                                      useLastOffset: Boolean = true) = this.synchronized {

    val consumerDefaults = defaults.TStreamsFactoryConsumerDefaults

    val consumerTransactionsPreload = pAsInt(co.Consumer.transactionPreload, consumerDefaults.Consumer.transactionPreload.default)
    consumerDefaults.Consumer.transactionPreload.check(consumerTransactionsPreload)

    val consumerDataPreload = pAsInt(co.Consumer.dataPreload, consumerDefaults.Consumer.dataPreload.default)
    consumerDefaults.Consumer.dataPreload.check(consumerDataPreload)

    val consumerOptions = new ConsumerOptions(transactionsPreload = consumerTransactionsPreload,
      dataPreload = consumerDataPreload,
      readPolicy = new RoundRobinPolicy(stream, partitions), offset = offset,
      transactionGenerator = transactionGenerator, useLastOffset = useLastOffset,
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

    assert(pAsString(co.Producer.bindPort) != null)
    assert(pAsString(co.Producer.bindHost) != null)
    assert(pAsString(co.Coordination.endpoints) != null)
    assert(pAsString(co.Coordination.prefix) != null)

    val port = getProperty(co.Producer.bindPort) match {
      case (p: Int) => p
      case PortRange(pFrom: Int, pTo: Int) => SpareServerSocketLookupUtility.findSparePort(pAsString(co.Producer.bindHost), pFrom, pTo).get
    }

    val coordinationDefaults = defaults.TStreamsFactoryCoordinationDefaults.Coordination
    val producerDefaults = defaults.TStreamsFactoryProducerDefaults.Producer

    val sessionTimeoutMs = pAsInt(co.Coordination.sessionTimeoutMs, coordinationDefaults.sessionTimeoutMs.default)
    coordinationDefaults.sessionTimeoutMs.check(sessionTimeoutMs)

    val connectionTimeoutMs = pAsInt(co.Coordination.connectionTimeoutMs, coordinationDefaults.connectionTimeoutMs.default)
    coordinationDefaults.connectionTimeoutMs.check(connectionTimeoutMs)

    val transportTimeoutMs = pAsInt(co.Producer.transportTimeoutMs, producerDefaults.transportTimeoutMs.default)
    producerDefaults.transportTimeoutMs.check(transportTimeoutMs)

    val transportRetryDelayMs = pAsInt(co.Producer.transportRetryDelayMs, producerDefaults.transportRetryDelayMs.default)
    producerDefaults.transportRetryDelayMs.check(transportRetryDelayMs)

    val transportRetryCount = pAsInt(co.Producer.transportRetryCount, producerDefaults.transportRetryCount.default)
    producerDefaults.transportRetryCount.check(transportRetryCount)

    val partitionsRedistributionDelaySec = pAsInt(co.Coordination.partitionsRedistributionDelaySec, coordinationDefaults.partitionsRedistributionDelaySec.default)
    coordinationDefaults.partitionsRedistributionDelaySec.check(partitionsRedistributionDelaySec)

    val threadPoolSize = pAsInt(co.Producer.threadPoolSize, producerDefaults.threadPoolSize.default)
    producerDefaults.threadPoolSize.check(threadPoolSize)

    val notifyThreadPoolSize = pAsInt(co.Producer.notifyThreadPoolSize, producerDefaults.notifyThreadPoolSize.default)
    producerDefaults.notifyThreadPoolSize.check(notifyThreadPoolSize)

    val transactionTtlMs = pAsInt(co.Producer.Transaction.ttlMs, producerDefaults.Transaction.ttlMs.default)
    producerDefaults.Transaction.ttlMs.check(transactionTtlMs)

    val transactionKeepAliveMs = pAsInt(co.Producer.Transaction.keepAliveMs, producerDefaults.Transaction.keepAliveMs.default)
    producerDefaults.Transaction.keepAliveMs.check(transactionKeepAliveMs)

    val batchSize = pAsInt(co.Producer.Transaction.batchSize, producerDefaults.Transaction.batchSize.default)
    producerDefaults.Transaction.batchSize.check(batchSize)

    val transport = new TcpTransport(
      pAsString(co.Producer.bindHost) + ":" + port.toString,
      transportTimeoutMs,
      transportRetryCount,
      transportRetryDelayMs)


    val cao = new CoordinationOptions(
      zkEndpoints = pAsString(co.Coordination.endpoints),
      zkPrefix = pAsString(co.Coordination.prefix),
      zkSessionTimeoutMs = sessionTimeoutMs,
      zkConnectionTimeoutMs = connectionTimeoutMs,
      transport = transport,
      threadPoolSize = threadPoolSize,
      notifyThreadPoolSize = notifyThreadPoolSize,
      partitionRedistributionDelaySec = partitionsRedistributionDelaySec
    )

    var writePolicy: AbstractPolicy = null

    if (pAsString(co.Producer.Transaction.distributionPolicy) ==
      co.Producer.Transaction.Consts.DISTRIBUTION_POLICY_RR) {
      writePolicy = new RoundRobinPolicy(stream, partitions)
    }
    else {
      throw new InvalidParameterException("Only TSF_Dictionary.Producer.Transaction.Consts.DISTRIBUTION_POLICY_RR policy " +
        "is supported currently in UniversalFactory.")
    }

    assert(transactionTtlMs >= transactionKeepAliveMs * 3)


    val po = new com.bwsw.tstreams.agents.producer.ProducerOptions(
      transactionTtlMs = transactionTtlMs,
      transactionKeepAliveMs = transactionKeepAliveMs,
      writePolicy = writePolicy,
      batchSize = batchSize,
      transactionGenerator = new LocalTransactionGenerator,
      coordinationOptions = cao)

    new Producer(name = name, stream = stream, producerOptions = po)
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
    val consumerOptions = getBasicConsumerOptions(transactionGenerator = new LocalTransactionGenerator,
      stream = stream, partitions = partitions,
      offset = offset, checkpointAtStart = checkpointAtStart,
      useLastOffset = useLastOffset)

    new Consumer(name, stream, consumerOptions)
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

    val consumerOptions = getBasicConsumerOptions(transactionGenerator = new LocalTransactionGenerator,
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
      case PortRange(pFrom: Int, pTo: Int) => SpareServerSocketLookupUtility.findSparePort(pAsString(co.Producer.bindHost), pFrom, pTo).get
    }

    val endpoints = pAsString(co.Coordination.endpoints)
    assert(endpoints != null)

    val root = pAsString(co.Coordination.prefix)
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

    val queue_path = pAsString(co.Consumer.Subscriber.persistentQueuePath)

    val opts = SubscriberOptionsBuilder.fromConsumerOptions(consumerOptions,
      agentAddress = bind_host + ":" + bind_port,
      zkPrefixPath = root,
      zkEndpoints = endpoints,
      zkSessionTimeoutMs = sessionTimeoutMs,
      zkConnectionTimeoutMs = connectionTimeoutMs,
      transactionsBufferWorkersThreadPoolSize = transactionBufferThreadPoolSize,
      processingEngineWorkersThreadSize = processingEnginesThreadPoolSize,
      pollingFrequencyDelayMs = pollingFrequencyDelayMs,
      transactionsQueueBuilder = if (queue_path == null) new QueueBuilder.InMemory() else new Persistent(queue_path))

    new Subscriber(name, stream, opts, callback)
  }

  /**
    * closes t-streams factory and stops further object creation
    */
  def close(): Unit = {
    if (isClosed.getAndSet(true))
      throw new IllegalStateException("TStreamsFactory is closed. This is repeatable close operation.")
  }

}
