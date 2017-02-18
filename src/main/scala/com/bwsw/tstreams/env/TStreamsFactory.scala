package com.bwsw.tstreams.env

import java.security.InvalidParameterException
import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreams.agents.consumer.Consumer
import com.bwsw.tstreams.agents.consumer.Offset.IOffset
import com.bwsw.tstreams.agents.consumer.subscriber.QueueBuilder.Persistent
import com.bwsw.tstreams.agents.consumer.subscriber.{QueueBuilder, Subscriber, SubscriberOptionsBuilder}
import com.bwsw.tstreams.agents.producer.{CoordinationOptions, Producer}
import com.bwsw.tstreams.common.{RoundRobinPolicy, _}
import com.bwsw.tstreams.converter.IConverter
import com.bwsw.tstreams.coordination.client.TcpTransport
import com.bwsw.tstreams.generator.ITransactionGenerator
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
  val isClosed    = new AtomicBoolean(false)
  val isLocked    = new AtomicBoolean(false)
  val co          = ConfigurationOptions

  propertyMap ++= defaults.TStreamsFactoryStorageDefaults.get
  propertyMap ++= defaults.TStreamsFactoryStreamDefaults.get
  propertyMap ++= defaults.TStreamsFactoryCoordinationDefaults.get

  // producer scope
  propertyMap += (co.Producer.BIND_HOST -> "localhost")
  propertyMap += (co.Producer.BIND_PORT ->(40000, 50000))
  val Producer_transport_timeout_default = 5
  val Producer_transport_timeout_min = 1
  val Producer_transport_timeout_max = 10
  propertyMap += (co.Producer.TRANSPORT_TIMEOUT -> Producer_transport_timeout_default)

  val Producer_transport_retry_count_default = 3
  val Producer_transport_retry_delay_default = 1

  propertyMap += (co.Producer.TRANSPORT_RETRY_COUNT -> Producer_transport_retry_count_default)
  propertyMap += (co.Producer.TRANSPORT_RETRY_DELAY -> Producer_transport_retry_delay_default)

  val Producer_transaction_ttl_default = 30
  val Producer_transaction_ttl_min = 3
  val Producer_transaction_ttl_max = 120
  propertyMap += (co.Producer.Transaction.TTL -> Producer_transaction_ttl_default)

  val Producer_transaction_open_maxwait_default = 5
  val Producer_transaction_open_maxwait_min = 1
  val Producer_transaction_open_maxwait_max = 10
  propertyMap += (co.Producer.Transaction.OPEN_MAXWAIT -> Producer_transaction_open_maxwait_default)

  val Producer_transaction_keep_alive_default = 1
  val Producer_transaction_keep_alive_min = 1
  val Producer_transaction_keep_alive_max = 2
  propertyMap += (co.Producer.Transaction.KEEP_ALIVE -> Producer_transaction_keep_alive_default)

  val Producer_transaction_data_write_batch_size_default = 100
  val Producer_transaction_data_write_batch_size_min = 1
  val Producer_transaction_data_write_batch_size_max = 1000
  propertyMap += (co.Producer.Transaction.DATA_WRITE_BATCH_SIZE -> Producer_transaction_data_write_batch_size_default)
  propertyMap += (co.Producer.Transaction.DISTRIBUTION_POLICY -> co.Producer.Transaction.Consts.DISTRIBUTION_POLICY_RR)

  val Producer_thread_pool_default = 4
  val Producer_thread_pool_min = 1
  val Producer_thread_pool_max = 64
  propertyMap += (co.Producer.THREAD_POOL -> Producer_thread_pool_default)

  val Producer_thread_pool_publisher_threads_amount_default = 1
  val Producer_thread_pool_publisher_threads_amount_min = 1
  val Producer_thread_pool_publisher_threads_amount_max = 32

  propertyMap += (co.Producer.THREAD_POOL_PUBLISHER_TREADS_AMOUNT -> Producer_thread_pool_publisher_threads_amount_default)

  // consumer scope
  val Consumer_transaction_preload_default = 10
  val Consumer_transaction_preload_min = 1
  val Consumer_transaction_preload_max = 100
  propertyMap += (co.Consumer.TRANSACTION_PRELOAD -> Consumer_transaction_preload_default)
  val Consumer_data_preload_default = 100
  val Consumer_data_preload_min = 10
  val Consumer_data_preload_max = 200
  propertyMap += (co.Consumer.DATA_PRELOAD -> Consumer_data_preload_default)
  propertyMap += (co.Consumer.Subscriber.BIND_HOST -> "localhost")
  propertyMap += (co.Consumer.Subscriber.BIND_PORT ->(40000, 50000))
  propertyMap += (co.Consumer.Subscriber.PERSISTENT_QUEUE_PATH -> null)

  val Subscriber_transaction_buffer_thread_pool_default = 4
  val Subscriber_transaction_buffer_thread_pool_min = 1
  val Subscriber_transaction_buffer_thread_pool_max = 64
  propertyMap += (co.Consumer.Subscriber.TRANSACTION_BUFFER_THREAD_POOL -> Subscriber_transaction_buffer_thread_pool_default)

  val Subscriber_processing_engines_thread_pool_default = 1
  val Subscriber_processing_engines_thread_pool_min = 1
  val Subscriber_processing_engines_thread_pool_max = 64
  propertyMap += (co.Consumer.Subscriber.PROCESSING_ENGINES_THREAD_POOL -> Subscriber_processing_engines_thread_pool_default)


  val Subscriber_polling_frequency_delay_default = 1000
  val Subscriber_polling_frequency_delay_min = 100
  val Subscriber_polling_frequency_delay_max = 100000
  propertyMap += (co.Consumer.Subscriber.POLLING_FREQUENCY_DELAY -> Subscriber_polling_frequency_delay_default)

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
  private def pAsInt(key: String, default: Int = 0): Int = if (null == getProperty(key)) default else Integer.parseInt(getProperty(key).toString)

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
    streamDefaults.Stream.ttl.check(pAsInt(co.Stream.ttl, streamDefaults.Stream.ttl.default))

    val clientOptions     = new ClientOptions()
    val authOptions       = new AuthOptions()
    val zookeeperOptions  = new ZookeeperOptions()

    // construct stream
    val stream = new Stream(
      storageClient   = new StorageClient(clientOptions = clientOptions, authOptions = authOptions, zookeeperOptions = zookeeperOptions),
      name            = pAsString(co.Stream.name),
      partitionsCount = pAsInt(co.Stream.partitionsCount, streamDefaults.Stream.partitionsCount.default),
      ttl             = pAsInt(co.Stream.ttl, streamDefaults.Stream.ttl.default),
      description     = pAsString(co.Stream.description, ""))

    stream
  }

  /**
    * reusable method which returns consumer options object
    */
  private def getBasicConsumerOptions[T](stream: Stream,
                                         partitions: Set[Int],
                                         converter: IConverter[Array[Byte], T],
                                         transactionGenerator: ITransactionGenerator,
                                         offset: IOffset,
                                         checkpointAtStart: Boolean = false,
                                         useLastOffset: Boolean = true): com.bwsw.tstreams.agents.consumer.ConsumerOptions[T] = this.synchronized {
    val consumer_transaction_preload = pAsInt(co.Consumer.TRANSACTION_PRELOAD, Consumer_transaction_preload_default)
    pAssertIntRange(consumer_transaction_preload, Consumer_transaction_preload_min, Consumer_transaction_preload_max)

    val consumer_data_preload = pAsInt(co.Consumer.DATA_PRELOAD, Consumer_data_preload_default)
    pAssertIntRange(consumer_data_preload, Consumer_data_preload_min, Consumer_data_preload_max)

    val consumerOptions = new com.bwsw.tstreams.agents.consumer.ConsumerOptions[T](transactionsPreload = consumer_transaction_preload,
      dataPreload           = consumer_data_preload, converter = converter,
      readPolicy            = new RoundRobinPolicy(stream, partitions), offset = offset,
      transactionGenerator  = transactionGenerator, useLastOffset = useLastOffset,
      checkpointAtStart     = checkpointAtStart)

    consumerOptions
  }

  /**
    * returns ready to use producer object
    *
    * @param name Producer name
    * @param transactionGenerator
    * @param converter
    * @param partitions
    * @tparam T - type convert data from
    * @return
    */
  def getProducer[T](name: String,
                     transactionGenerator: ITransactionGenerator,
                     converter: IConverter[T, Array[Byte]],
                     partitions: Set[Int]
                    ): Producer[T] = this.synchronized {

    if (isClosed.get)
      throw new IllegalStateException("TStreamsFactory is closed. This is the illegal usage of the object.")


    val stream: Stream[Array[Byte]] = getStream()

    assert(pAsString(co.Producer.BIND_PORT) != null)
    assert(pAsString(co.Producer.BIND_HOST) != null)
    assert(pAsString(co.Coordination.endpoints) != null)
    assert(pAsString(co.Coordination.prefix) != null)

    val port = getProperty(co.Producer.BIND_PORT) match {
      case (p: Int) => p
      case (pFrom: Int, pTo: Int) => SpareServerSocketLookupUtility.findSparePort(pAsString(co.Producer.BIND_HOST), pFrom, pTo).get
    }

    pAssertIntRange(pAsInt(co.Coordination.sessionTimeoutMs, Coordination_ttl_default), Coordination_ttl_min, Coordination_ttl_max)

    pAssertIntRange(pAsInt(co.Producer.TRANSPORT_TIMEOUT, Producer_transport_timeout_default), Producer_transport_timeout_min, Producer_transport_timeout_max)

    pAssertIntRange(pAsInt(co.Coordination.connectionTimeoutMs, Coordination_connection_timeout_default),
      Coordination_connection_timeout_min, Coordination_connection_timeout_max)

    pAssertIntRange(pAsInt(co.Coordination.partitionsRedistributionDelaySec, Coordination_partition_redistribution_delay_default),
      Coordination_partition_redistribution_delay_min, Coordination_partition_redistribution_delay_max)

    pAssertIntRange(pAsInt(co.Producer.THREAD_POOL, Producer_thread_pool_default), Producer_thread_pool_min, Producer_thread_pool_max)

    pAssertIntRange(pAsInt(co.Producer.THREAD_POOL_PUBLISHER_TREADS_AMOUNT, Producer_thread_pool_publisher_threads_amount_default),
      Producer_thread_pool_publisher_threads_amount_min, Producer_thread_pool_publisher_threads_amount_max)

    val transport = new TcpTransport(
      pAsString(co.Producer.BIND_HOST) + ":" + port.toString,
      pAsInt(co.Producer.TRANSPORT_TIMEOUT, Producer_transport_timeout_default) * 1000,
      pAsInt(co.Producer.TRANSPORT_RETRY_COUNT, Producer_transport_retry_count_default),
      pAsInt(co.Producer.TRANSPORT_RETRY_DELAY, Producer_transport_retry_delay_default) * 1000)


    val cao = new CoordinationOptions(
      zkHosts = pAsString(co.Coordination.endpoints),
      zkRootPath = pAsString(co.Coordination.prefix),
      zkSessionTimeout = pAsInt(co.Coordination.sessionTimeoutMs, Coordination_ttl_default),
      zkConnectionTimeout = pAsInt(co.Coordination.connectionTimeoutMs, Coordination_connection_timeout_default),
      transport = transport,
      threadPoolAmount = pAsInt(co.Producer.THREAD_POOL, Producer_thread_pool_default),
      threadPoolPublisherThreadsAmount = pAsInt(co.Producer.THREAD_POOL_PUBLISHER_TREADS_AMOUNT, Producer_thread_pool_publisher_threads_amount_default),
      partitionRedistributionDelay = pAsInt(co.Coordination.partitionsRedistributionDelaySec, Coordination_partition_redistribution_delay_default)
    )


    var writePolicy: AbstractPolicy = null

    if (pAsString(co.Producer.Transaction.DISTRIBUTION_POLICY) ==
      co.Producer.Transaction.Consts.DISTRIBUTION_POLICY_RR) {
      writePolicy = new RoundRobinPolicy(stream, partitions)
    }
    else {
      throw new InvalidParameterException("Only TSF_Dictionary.Producer.Transaction.Consts.DISTRIBUTION_POLICY_RR policy " +
        "is supported currently in UniversalFactory.")
    }

    pAssertIntRange(pAsInt(co.Producer.Transaction.TTL, Producer_transaction_ttl_default), Producer_transaction_ttl_min, Producer_transaction_ttl_max)
    pAssertIntRange(pAsInt(co.Producer.Transaction.KEEP_ALIVE, Producer_transaction_keep_alive_default), Producer_transaction_keep_alive_min, Producer_transaction_keep_alive_max)
    assert(pAsInt(co.Producer.Transaction.TTL, Producer_transaction_ttl_default) >=
      pAsInt(co.Producer.Transaction.KEEP_ALIVE, Producer_transaction_keep_alive_default) * 3)

    val insertCnt = pAsInt(co.Producer.Transaction.DATA_WRITE_BATCH_SIZE, Producer_transaction_data_write_batch_size_default)
    pAssertIntRange(insertCnt,
      Producer_transaction_data_write_batch_size_min, Producer_transaction_data_write_batch_size_max)

    val po = new com.bwsw.tstreams.agents.producer.ProducerOptions[T](
      transactionTTL = pAsInt(co.Producer.Transaction.TTL, Producer_transaction_ttl_default),
      transactionKeepAliveInterval = pAsInt(co.Producer.Transaction.KEEP_ALIVE, Producer_transaction_keep_alive_default),
      writePolicy = writePolicy,
      batchSize = insertCnt,
      transactionGenerator = transactionGenerator,
      coordinationOptions = cao,
      converter = converter)

    new Producer[T](name = name, stream = stream, producerOptions = po)
  }

  /**
    * returns ready to use consumer object
    *
    * @param name Consumer name
    * @param transactionGenerator
    * @param converter
    * @param partitions
    * @tparam T type to convert data to
    * @return
    */
  def getConsumer[T](name: String,
                     transactionGenerator: ITransactionGenerator,
                     converter: IConverter[Array[Byte], T],
                     partitions: Set[Int],
                     offset: IOffset,
                     useLastOffset: Boolean = true,
                     checkpointAtStart: Boolean = false): Consumer[T] = this.synchronized {

    if (isClosed.get)
      throw new IllegalStateException("TStreamsFactory is closed. This is the illegal usage of the object.")


    val stream: Stream[Array[Byte]] = getStream()
    val consumerOptions = getBasicConsumerOptions(transactionGenerator = transactionGenerator,
      stream = stream, partitions = partitions, converter = converter,
      offset = offset, checkpointAtStart = checkpointAtStart,
      useLastOffset = useLastOffset)

    new Consumer(name, stream, consumerOptions)
  }


  /**
    * returns ready to use subscribing consumer object
    *
    * @param transactionGenerator
    * @param converter
    * @param partitions
    * @param callback
    * @tparam T - type to convert data to
    * @return
    */
  def getSubscriber[T](name: String,
                       transactionGenerator: ITransactionGenerator,
                       converter: IConverter[Array[Byte], T],
                       partitions: Set[Int],
                       callback: com.bwsw.tstreams.agents.consumer.subscriber.Callback[T],
                       offset: IOffset,
                       useLastOffset: Boolean = true,
                       checkpointAtStart: Boolean = false): Subscriber[T] = this.synchronized {
    if (isClosed.get)
      throw new IllegalStateException("TStreamsFactory is closed. This is the illegal usage of the object.")

    val stream: Stream[Array[Byte]] = getStream()

    val consumerOptions = getBasicConsumerOptions(transactionGenerator = transactionGenerator,
      stream = stream,
      partitions = partitions,
      converter = converter,
      checkpointAtStart = checkpointAtStart,
      offset = offset,
      useLastOffset = useLastOffset)

    val bind_host = pAsString(co.Consumer.Subscriber.BIND_HOST)
    assert(bind_host != null)
    assert(co.Consumer.Subscriber.BIND_PORT != null)

    val bind_port = getProperty(co.Consumer.Subscriber.BIND_PORT) match {
      case (p: Int) => p
      case (pFrom: Int, pTo: Int) => SpareServerSocketLookupUtility.findSparePort(pAsString(co.Producer.BIND_HOST), pFrom, pTo).get
    }

    val endpoints = pAsString(co.Coordination.endpoints)
    assert(endpoints != null)

    val root = pAsString(co.Coordination.prefix)
    assert(root != null)

    val ttl = pAsInt(co.Coordination.sessionTimeoutMs, Coordination_ttl_default)
    pAssertIntRange(ttl, Coordination_ttl_min, Coordination_ttl_max)
    val conn_timeout = pAsInt(co.Coordination.connectionTimeoutMs, Coordination_connection_timeout_default)
    pAssertIntRange(conn_timeout,
      Coordination_connection_timeout_min, Coordination_connection_timeout_max)

    val transaction_thread_pool = pAsInt(co.Consumer.Subscriber.TRANSACTION_BUFFER_THREAD_POOL, Subscriber_transaction_buffer_thread_pool_default)
    pAssertIntRange(transaction_thread_pool,
      Subscriber_transaction_buffer_thread_pool_min, Subscriber_transaction_buffer_thread_pool_max)

    val pe_thread_pool = pAsInt(co.Consumer.Subscriber.PROCESSING_ENGINES_THREAD_POOL, Subscriber_processing_engines_thread_pool_default)
    pAssertIntRange(pe_thread_pool,
      Subscriber_processing_engines_thread_pool_min, Subscriber_processing_engines_thread_pool_max)

    val polling_frequency = pAsInt(co.Consumer.Subscriber.POLLING_FREQUENCY_DELAY, Subscriber_polling_frequency_delay_default)
    pAssertIntRange(polling_frequency,
      Subscriber_polling_frequency_delay_min, Subscriber_polling_frequency_delay_max)

    val queue_path = pAsString(co.Consumer.Subscriber.PERSISTENT_QUEUE_PATH)

    val opts = SubscriberOptionsBuilder.fromConsumerOptions(consumerOptions,
      agentAddress = bind_host + ":" + bind_port,
      zkRootPath = root,
      zkHosts = endpoints,
      zkSessionTimeout = ttl,
      zkConnectionTimeout = conn_timeout,
      transactionsBufferWorkersThreadPoolAmount = transaction_thread_pool,
      processingEngineWorkersThreadAmount = pe_thread_pool,
      pollingFrequencyDelay = polling_frequency,
      transactionsQueueBuilder = if (queue_path == null) new QueueBuilder.InMemory() else new Persistent(queue_path))

    new Subscriber[T](name, stream, opts, callback)
  }

  /**
    * closes t-streams factory and stops further object creation
    */
  def close(): Unit = {
    if (isClosed.getAndSet(true))
      throw new IllegalStateException("TStreamsFactory is closed. This is repeatable close operation.")

  }

}
