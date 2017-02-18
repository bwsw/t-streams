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

case class IntMinMaxDefault(min: Int = 0, max: Int = 0, default: Int = 0) {
  def check(v: Int) = {
    assert(v >= min && v <= max)
    v
  }
}

/**
  * Class which holds definitions for UniversalFactory
  */
object ConfigurationOptions {

  /**
    * TSF_Dictionary storage scope
    */
  object StorageClient {

    object Zookeeper {
      val endpoints             = "storage-client.zk.endpoints"
      val prefix                = "storage-client.zk.prefix"
      val sessionTimeoutMs      = "storage-client.zk.session-timeout-ms"
      val connectionTimeoutMs   = "storage-client.zk.connection-timeout-ms"
      val retryCount            = "storage-client.zk.retry-count"
      val retryDelayMs          = "storage-client.zk.retry-delay-ms"
    }

    // TODO: fixit bad scope
    object Auth {
      val key                       = "storage-client.auth.key"
      val connectionTimeoutMs       = "storage-client.auth.connection-timeout-ms"
      val retryDelayMs              = "storage-client.auth.retry-delay-ms"
      val tokenConnectionTimeoutMs  = "storage-client.auth.token-connection-timeout-ms"
      val tokenRetryDelayMs         = "storage-client.auth.token-retry-delay-ms"
    }

    val connectionTimeoutMs         = "storage-client.connection-timeout-ms"
    val retryDelayMs                = "storage-client.retry-delay-ms"
    val threadPool                  = "storage-client.thread-pool"
  }

  /**
    * TSF_Dictionary coordination scope
    */
  object Coordination {
    /**
      * endpoint list for zookeeper coordination service, comma separated: host1:port1,host2:port2,host3:port3,...
      */
    val ENDPOINTS = "coordination.endpoints"
    /**
      * ZK root node which holds coordination tree
      */
    val ROOT = "coordination.root"
    /**
      * ZK ttl for coordination
      */
    val TTL = "coordination.ttl"

    /**
      * ZK connection timeout
      */
    val CONNECTION_TIMEOUT = "coordination.connection-timeout"

    /**
      * partition redistribution delay
      */
    val PARTITION_REDISTRIBUTION_DELAY = "coordination.partition-redistribution-delay"
  }

  /**
    * TSF_Dictionary stream scope
    */
  object Stream {
    /**
      * name of the stream to work with
      */
    val NAME = "stream.name"
    /**
      * amount of stream partitions
      */
    val PARTITIONS = "stream.partitions"
    /**
      * stream time to leave (data expunged from datastore after that time)
      */
    val TTL = "stream.ttl"
    /**
      * random string description
      */
    val DESCRIPTION = "stream.description"
  }

  /**
    * TSF_Dictionary producer scope
    */
  object Producer {

    /**
      * amount of threads which handles works with transactions on master
      */
    val THREAD_POOL = "producer.thread-pool"

    /**
      * amount of publisher threads in a thread pool (default 1)
      */
    val THREAD_POOL_PUBLISHER_TREADS_AMOUNT = "producer.thread-pool.publisher-threads-amount"

    /**
      * hostname or ip of producer master listener
      */
    val BIND_HOST = "producer.bind-host"
    /**
      * port of producer master listener
      */
    val BIND_PORT = "producer.bind-port"
    /**
      * Transport timeout is maximum time to wait for master to respond
      */
    val TRANSPORT_TIMEOUT = "producer.transport-timeout"

    /**
      * Retry count for transport failures
      */
    val TRANSPORT_RETRY_COUNT = "producer.transport-retry-count"

    /**
      * Retry delay for transport failures
      */
    val TRANSPORT_RETRY_DELAY = "producer.transport-retry-delay"


    object Transaction {
      /**
        * TTL of transaction to wait until determine it's broken
        */
      val TTL = "producer.transaction.ttl"
      /**
        * Time to wait for successful end of opening operation on master for transaction
        */
      val OPEN_MAXWAIT = "producer.transaction.open-maxwait"
      /**
        * Time to update transaction state (keep it alive for long transactions)
        */
      val KEEP_ALIVE = "producer.transaction.keep-alive"
      /**
        * amount of data items to batch when write data into transaction
        */
      val DATA_WRITE_BATCH_SIZE = "producer.transaction.data-write-batch-size"
      /**
        * policy to distribute transactions over stream partitions
        */
      val DISTRIBUTION_POLICY = "producer.transaction.distribution-policy"

      // TODO: fix internals write->distribution

      /**
        * TSF_Dictionary.Producer.Transaction consts scope
        */
      object Consts {
        /**
          * defines standard round-robin policy
          */
        val DISTRIBUTION_POLICY_RR = "round-robin"
      }

    }


  }

  /**
    * TSF_Dictionary consumer scope
    */
  object Consumer {
    /**
      * amount of transactions to preload from C* to avoid additional select ops
      */
    val TRANSACTION_PRELOAD = "consumer.transaction-preload"
    /**
      * amount of data items to load at once from data storage
      */
    val DATA_PRELOAD = "consumer.data-preload"

    /**
      * TSF_Dictionary.Consumer subscriber scope
      */
    object Subscriber {
      /**
        * host/ip to bind
        */
      val BIND_HOST = "consumer.subscriber.bind-host"
      /**
        * port to bind
        */
      val BIND_PORT = "consumer.subscriber.bind-port"

      /**
        * persistent queue path (fast disk where to store bursted data
        */
      val PERSISTENT_QUEUE_PATH = "consumer.subscriber.persistent-queue.path"

      /**
        * thread pool size
        */
      val TRANSACTION_BUFFER_THREAD_POOL = "consumer.subscriber.transaction-buffer-thread-pool"

      /**
        * processing engines pool
        */
      val PROCESSING_ENGINES_THREAD_POOL = "consumer.subscriber.processing-engines-thread-pool"

      /**
        * thread pool size
        */
      val POLLING_FREQUENCY_DELAY = "consumer.subscriber.polling-frequency-delay"

    }

  }

}

/**
  * Created by Ivan Kudryavtsev on 21.07.16.
  */
class TStreamsFactory() {

  private val logger = LoggerFactory.getLogger(this.getClass)
  val propertyMap = mutable.HashMap[String, Any]()
  val isClosed = new AtomicBoolean(false)
  val isLocked = new AtomicBoolean(false)

  propertyMap ++= TStreamsFactoryStorageDefaults.get

  // coordination scope
  propertyMap += (ConfigurationOptions.Coordination.ENDPOINTS -> "localhost:2181")
  propertyMap += (ConfigurationOptions.Coordination.ROOT -> "/t-streams")

  val Coordination_ttl_default = 5
  val Coordination_ttl_min = 1
  val Coordination_ttl_max = 10
  propertyMap += (ConfigurationOptions.Coordination.TTL -> Coordination_ttl_default)

  val Coordination_connection_timeout_default = 5
  val Coordination_connection_timeout_min = 1
  val Coordination_connection_timeout_max = 10
  propertyMap += (ConfigurationOptions.Coordination.CONNECTION_TIMEOUT -> Coordination_connection_timeout_default)

  val Coordination_partition_redistribution_delay_default = 2
  val Coordination_partition_redistribution_delay_min = 1
  val Coordination_partition_redistribution_delay_max = 100
  propertyMap += (ConfigurationOptions.Coordination.PARTITION_REDISTRIBUTION_DELAY -> Coordination_partition_redistribution_delay_default)


  // stream scope
  propertyMap += (ConfigurationOptions.Stream.NAME -> "test")

  val Stream_partitions_default = 1
  val Stream_partitions_min = 1
  val Stream_partitions_max = 100000000
  propertyMap += (ConfigurationOptions.Stream.PARTITIONS -> Stream_partitions_default)

  val Stream_ttl_default = 60 * 60 * 24
  val Stream_ttl_min = 60
  val Stream_ttl_max = 315360000
  propertyMap += (ConfigurationOptions.Stream.TTL -> Stream_ttl_default)
  propertyMap += (ConfigurationOptions.Stream.DESCRIPTION -> "Test stream")

  // producer scope
  propertyMap += (ConfigurationOptions.Producer.BIND_HOST -> "localhost")
  propertyMap += (ConfigurationOptions.Producer.BIND_PORT ->(40000, 50000))
  val Producer_transport_timeout_default = 5
  val Producer_transport_timeout_min = 1
  val Producer_transport_timeout_max = 10
  propertyMap += (ConfigurationOptions.Producer.TRANSPORT_TIMEOUT -> Producer_transport_timeout_default)

  val Producer_transport_retry_count_default = 3
  val Producer_transport_retry_delay_default = 1

  propertyMap += (ConfigurationOptions.Producer.TRANSPORT_RETRY_COUNT -> Producer_transport_retry_count_default)
  propertyMap += (ConfigurationOptions.Producer.TRANSPORT_RETRY_DELAY -> Producer_transport_retry_delay_default)

  val Producer_transaction_ttl_default = 30
  val Producer_transaction_ttl_min = 3
  val Producer_transaction_ttl_max = 120
  propertyMap += (ConfigurationOptions.Producer.Transaction.TTL -> Producer_transaction_ttl_default)

  val Producer_transaction_open_maxwait_default = 5
  val Producer_transaction_open_maxwait_min = 1
  val Producer_transaction_open_maxwait_max = 10
  propertyMap += (ConfigurationOptions.Producer.Transaction.OPEN_MAXWAIT -> Producer_transaction_open_maxwait_default)

  val Producer_transaction_keep_alive_default = 1
  val Producer_transaction_keep_alive_min = 1
  val Producer_transaction_keep_alive_max = 2
  propertyMap += (ConfigurationOptions.Producer.Transaction.KEEP_ALIVE -> Producer_transaction_keep_alive_default)

  val Producer_transaction_data_write_batch_size_default = 100
  val Producer_transaction_data_write_batch_size_min = 1
  val Producer_transaction_data_write_batch_size_max = 1000
  propertyMap += (ConfigurationOptions.Producer.Transaction.DATA_WRITE_BATCH_SIZE -> Producer_transaction_data_write_batch_size_default)
  propertyMap += (ConfigurationOptions.Producer.Transaction.DISTRIBUTION_POLICY -> ConfigurationOptions.Producer.Transaction.Consts.DISTRIBUTION_POLICY_RR)

  val Producer_thread_pool_default = 4
  val Producer_thread_pool_min = 1
  val Producer_thread_pool_max = 64
  propertyMap += (ConfigurationOptions.Producer.THREAD_POOL -> Producer_thread_pool_default)

  val Producer_thread_pool_publisher_threads_amount_default = 1
  val Producer_thread_pool_publisher_threads_amount_min = 1
  val Producer_thread_pool_publisher_threads_amount_max = 32

  propertyMap += (ConfigurationOptions.Producer.THREAD_POOL_PUBLISHER_TREADS_AMOUNT -> Producer_thread_pool_publisher_threads_amount_default)

  // consumer scope
  val Consumer_transaction_preload_default = 10
  val Consumer_transaction_preload_min = 1
  val Consumer_transaction_preload_max = 100
  propertyMap += (ConfigurationOptions.Consumer.TRANSACTION_PRELOAD -> Consumer_transaction_preload_default)
  val Consumer_data_preload_default = 100
  val Consumer_data_preload_min = 10
  val Consumer_data_preload_max = 200
  propertyMap += (ConfigurationOptions.Consumer.DATA_PRELOAD -> Consumer_data_preload_default)
  propertyMap += (ConfigurationOptions.Consumer.Subscriber.BIND_HOST -> "localhost")
  propertyMap += (ConfigurationOptions.Consumer.Subscriber.BIND_PORT ->(40000, 50000))
  propertyMap += (ConfigurationOptions.Consumer.Subscriber.PERSISTENT_QUEUE_PATH -> null)

  val Subscriber_transaction_buffer_thread_pool_default = 4
  val Subscriber_transaction_buffer_thread_pool_min = 1
  val Subscriber_transaction_buffer_thread_pool_max = 64
  propertyMap += (ConfigurationOptions.Consumer.Subscriber.TRANSACTION_BUFFER_THREAD_POOL -> Subscriber_transaction_buffer_thread_pool_default)

  val Subscriber_processing_engines_thread_pool_default = 1
  val Subscriber_processing_engines_thread_pool_min = 1
  val Subscriber_processing_engines_thread_pool_max = 64
  propertyMap += (ConfigurationOptions.Consumer.Subscriber.PROCESSING_ENGINES_THREAD_POOL -> Subscriber_processing_engines_thread_pool_default)


  val Subscriber_polling_frequency_delay_default = 1000
  val Subscriber_polling_frequency_delay_min = 100
  val Subscriber_polling_frequency_delay_max = 100000
  propertyMap += (ConfigurationOptions.Consumer.Subscriber.POLLING_FREQUENCY_DELAY -> Subscriber_polling_frequency_delay_default)

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
    * checks that int inside interval
    *
    * @param value
    * @param min
    * @param max
    * @return
    */
  private def pAssertIntRange(value: Int, min: Int, max: Int): Int = {
    assert(value >= min && value <= max)
    value
  }



  /**
    * common routine which allows to get ready to use stream object by env
    *
    * @return
    */
  private def getStream(): Stream[Array[Byte]] = this.synchronized {

    if (isClosed.get)
      throw new IllegalStateException("TStreamsFactory is closed. This is the illegal usage of the object.")

    assert(pAsString(ConfigurationOptions.Stream.NAME) != null)
    pAssertIntRange(pAsInt(ConfigurationOptions.Stream.PARTITIONS, Stream_partitions_default), Stream_partitions_min, Stream_partitions_max)
    pAssertIntRange(pAsInt(ConfigurationOptions.Stream.TTL, Stream_ttl_default), Stream_ttl_min, Stream_ttl_max)

    val clientOptions     = new ClientOptions()
    val authOptions       = new AuthOptions()
    val zookeeperOptions  = new ZookeeperOptions()

    // construct stream
    val stream = new Stream[Array[Byte]](
      storageClient   = new StorageClient(clientOptions = clientOptions, authOptions = authOptions, zookeeperOptions = zookeeperOptions),
      name            = pAsString(ConfigurationOptions.Stream.NAME),
      partitionsCount = pAsInt(ConfigurationOptions.Stream.PARTITIONS, Stream_partitions_default),
      ttl             = pAsInt(ConfigurationOptions.Stream.TTL, Stream_ttl_default),
      description     = pAsString(ConfigurationOptions.Stream.DESCRIPTION, ""))
    return stream
  }

  /**
    * reusable method which returns consumer options object
    */
  private def getBasicConsumerOptions[T](stream: Stream[Array[Byte]],
                                         partitions: Set[Int],
                                         converter: IConverter[Array[Byte], T],
                                         transactionGenerator: ITransactionGenerator,
                                         offset: IOffset,
                                         checkpointAtStart: Boolean = false,
                                         useLastOffset: Boolean = true): com.bwsw.tstreams.agents.consumer.ConsumerOptions[T] = this.synchronized {
    val consumer_transaction_preload = pAsInt(ConfigurationOptions.Consumer.TRANSACTION_PRELOAD, Consumer_transaction_preload_default)
    pAssertIntRange(consumer_transaction_preload, Consumer_transaction_preload_min, Consumer_transaction_preload_max)

    val consumer_data_preload = pAsInt(ConfigurationOptions.Consumer.DATA_PRELOAD, Consumer_data_preload_default)
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

    assert(pAsString(ConfigurationOptions.Producer.BIND_PORT) != null)
    assert(pAsString(ConfigurationOptions.Producer.BIND_HOST) != null)
    assert(pAsString(ConfigurationOptions.Coordination.ENDPOINTS) != null)
    assert(pAsString(ConfigurationOptions.Coordination.ROOT) != null)

    val port = getProperty(ConfigurationOptions.Producer.BIND_PORT) match {
      case (p: Int) => p
      case (pFrom: Int, pTo: Int) => SpareServerSocketLookupUtility.findSparePort(pAsString(ConfigurationOptions.Producer.BIND_HOST), pFrom, pTo).get
    }

    pAssertIntRange(pAsInt(ConfigurationOptions.Coordination.TTL, Coordination_ttl_default), Coordination_ttl_min, Coordination_ttl_max)

    pAssertIntRange(pAsInt(ConfigurationOptions.Producer.TRANSPORT_TIMEOUT, Producer_transport_timeout_default), Producer_transport_timeout_min, Producer_transport_timeout_max)

    pAssertIntRange(pAsInt(ConfigurationOptions.Coordination.CONNECTION_TIMEOUT, Coordination_connection_timeout_default),
      Coordination_connection_timeout_min, Coordination_connection_timeout_max)

    pAssertIntRange(pAsInt(ConfigurationOptions.Coordination.PARTITION_REDISTRIBUTION_DELAY, Coordination_partition_redistribution_delay_default),
      Coordination_partition_redistribution_delay_min, Coordination_partition_redistribution_delay_max)

    pAssertIntRange(pAsInt(ConfigurationOptions.Producer.THREAD_POOL, Producer_thread_pool_default), Producer_thread_pool_min, Producer_thread_pool_max)

    pAssertIntRange(pAsInt(ConfigurationOptions.Producer.THREAD_POOL_PUBLISHER_TREADS_AMOUNT, Producer_thread_pool_publisher_threads_amount_default),
      Producer_thread_pool_publisher_threads_amount_min, Producer_thread_pool_publisher_threads_amount_max)

    val transport = new TcpTransport(
      pAsString(ConfigurationOptions.Producer.BIND_HOST) + ":" + port.toString,
      pAsInt(ConfigurationOptions.Producer.TRANSPORT_TIMEOUT, Producer_transport_timeout_default) * 1000,
      pAsInt(ConfigurationOptions.Producer.TRANSPORT_RETRY_COUNT, Producer_transport_retry_count_default),
      pAsInt(ConfigurationOptions.Producer.TRANSPORT_RETRY_DELAY, Producer_transport_retry_delay_default) * 1000)


    val cao = new CoordinationOptions(
      zkHosts = pAsString(ConfigurationOptions.Coordination.ENDPOINTS),
      zkRootPath = pAsString(ConfigurationOptions.Coordination.ROOT),
      zkSessionTimeout = pAsInt(ConfigurationOptions.Coordination.TTL, Coordination_ttl_default),
      zkConnectionTimeout = pAsInt(ConfigurationOptions.Coordination.CONNECTION_TIMEOUT, Coordination_connection_timeout_default),
      transport = transport,
      threadPoolAmount = pAsInt(ConfigurationOptions.Producer.THREAD_POOL, Producer_thread_pool_default),
      threadPoolPublisherThreadsAmount = pAsInt(ConfigurationOptions.Producer.THREAD_POOL_PUBLISHER_TREADS_AMOUNT, Producer_thread_pool_publisher_threads_amount_default),
      partitionRedistributionDelay = pAsInt(ConfigurationOptions.Coordination.PARTITION_REDISTRIBUTION_DELAY, Coordination_partition_redistribution_delay_default)
    )


    var writePolicy: AbstractPolicy = null

    if (pAsString(ConfigurationOptions.Producer.Transaction.DISTRIBUTION_POLICY) ==
      ConfigurationOptions.Producer.Transaction.Consts.DISTRIBUTION_POLICY_RR) {
      writePolicy = new RoundRobinPolicy(stream, partitions)
    }
    else {
      throw new InvalidParameterException("Only TSF_Dictionary.Producer.Transaction.Consts.DISTRIBUTION_POLICY_RR policy " +
        "is supported currently in UniversalFactory.")
    }

    pAssertIntRange(pAsInt(ConfigurationOptions.Producer.Transaction.TTL, Producer_transaction_ttl_default), Producer_transaction_ttl_min, Producer_transaction_ttl_max)
    pAssertIntRange(pAsInt(ConfigurationOptions.Producer.Transaction.KEEP_ALIVE, Producer_transaction_keep_alive_default), Producer_transaction_keep_alive_min, Producer_transaction_keep_alive_max)
    assert(pAsInt(ConfigurationOptions.Producer.Transaction.TTL, Producer_transaction_ttl_default) >=
      pAsInt(ConfigurationOptions.Producer.Transaction.KEEP_ALIVE, Producer_transaction_keep_alive_default) * 3)

    val insertCnt = pAsInt(ConfigurationOptions.Producer.Transaction.DATA_WRITE_BATCH_SIZE, Producer_transaction_data_write_batch_size_default)
    pAssertIntRange(insertCnt,
      Producer_transaction_data_write_batch_size_min, Producer_transaction_data_write_batch_size_max)

    val po = new com.bwsw.tstreams.agents.producer.ProducerOptions[T](
      transactionTTL = pAsInt(ConfigurationOptions.Producer.Transaction.TTL, Producer_transaction_ttl_default),
      transactionKeepAliveInterval = pAsInt(ConfigurationOptions.Producer.Transaction.KEEP_ALIVE, Producer_transaction_keep_alive_default),
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

    val bind_host = pAsString(ConfigurationOptions.Consumer.Subscriber.BIND_HOST)
    assert(bind_host != null)
    assert(ConfigurationOptions.Consumer.Subscriber.BIND_PORT != null)

    val bind_port = getProperty(ConfigurationOptions.Consumer.Subscriber.BIND_PORT) match {
      case (p: Int) => p
      case (pFrom: Int, pTo: Int) => SpareServerSocketLookupUtility.findSparePort(pAsString(ConfigurationOptions.Producer.BIND_HOST), pFrom, pTo).get
    }

    val endpoints = pAsString(ConfigurationOptions.Coordination.ENDPOINTS)
    assert(endpoints != null)

    val root = pAsString(ConfigurationOptions.Coordination.ROOT)
    assert(root != null)

    val ttl = pAsInt(ConfigurationOptions.Coordination.TTL, Coordination_ttl_default)
    pAssertIntRange(ttl, Coordination_ttl_min, Coordination_ttl_max)
    val conn_timeout = pAsInt(ConfigurationOptions.Coordination.CONNECTION_TIMEOUT, Coordination_connection_timeout_default)
    pAssertIntRange(conn_timeout,
      Coordination_connection_timeout_min, Coordination_connection_timeout_max)

    val transaction_thread_pool = pAsInt(ConfigurationOptions.Consumer.Subscriber.TRANSACTION_BUFFER_THREAD_POOL, Subscriber_transaction_buffer_thread_pool_default)
    pAssertIntRange(transaction_thread_pool,
      Subscriber_transaction_buffer_thread_pool_min, Subscriber_transaction_buffer_thread_pool_max)

    val pe_thread_pool = pAsInt(ConfigurationOptions.Consumer.Subscriber.PROCESSING_ENGINES_THREAD_POOL, Subscriber_processing_engines_thread_pool_default)
    pAssertIntRange(pe_thread_pool,
      Subscriber_processing_engines_thread_pool_min, Subscriber_processing_engines_thread_pool_max)

    val polling_frequency = pAsInt(ConfigurationOptions.Consumer.Subscriber.POLLING_FREQUENCY_DELAY, Subscriber_polling_frequency_delay_default)
    pAssertIntRange(polling_frequency,
      Subscriber_polling_frequency_delay_min, Subscriber_polling_frequency_delay_max)

    val queue_path = pAsString(ConfigurationOptions.Consumer.Subscriber.PERSISTENT_QUEUE_PATH)

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
