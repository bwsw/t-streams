package com.bwsw.tstreams.env

import java.net.InetSocketAddress
import java.security.InvalidParameterException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import com.aerospike.client.Host
import com.aerospike.client.policy.{ClientPolicy, Policy, WritePolicy}
import com.bwsw.tstreams.agents.consumer.Offset.IOffset
import com.bwsw.tstreams.agents.consumer.subscriber.{Callback, SubscribingConsumer}
import com.bwsw.tstreams.agents.consumer.{Consumer, Options, SubscriberCoordinationOptions}
import com.bwsw.tstreams.agents.producer.{CoordinationOptions, Options, Producer}
import com.bwsw.tstreams.common._
import com.bwsw.tstreams.converter.IConverter
import com.bwsw.tstreams.coordination.producer.transport.impl.TcpTransport
import com.bwsw.tstreams.data.IStorage
import com.bwsw.tstreams.generator.IUUIDGenerator
import com.bwsw.tstreams.metadata.{MetadataStorage, MetadataStorageFactory}
import com.bwsw.tstreams.common.RoundRobinPolicy
import com.bwsw.tstreams.streams.TStream
import org.slf4j.LoggerFactory
import scala.collection.mutable.HashMap

/**
  * Class which holds definitions for UniversalFactory
  */
object TSF_Dictionary {


  /**
    * TSF_Dictionary metadata scope
    */
  object Metadata {

    /**
      * TSF_Dictionary.Metadata cluster scope
      */
    object Cluster {
      /**
        * endpoint list of the metadata datastore, comma separated: host1:port1,host2:port2,host3:port3,...
        */
      val ENDPOINTS = "metadata.cluster.endpoints"

      /**
        * keyspace for metadata storage
        */
      val NAMESPACE = "metadata.cluster.namespace"
      /**
        * login of the user which can access to the metadata store
        */
      val LOGIN = "metadata.cluster.login"
      /**
        * password of the user which can access to the metadata store
        */
      val PASSWORD = "metadata.cluster.password"

      /**
        * Local DC
        */
      val LOCAL_DC = "metadata.cluster.local-dc"

      /**
        * Connection keep alive in MS
        */
      val KEEP_ALIVE_MS = "metadata.cluster.keep-alive-ms"

      /**
        * Minimal reconnection delay
        */
      val MIN_RECONNECTION_DELAY_MS = "metadata.cluster.min-reconnection-delay-ms"

      /**
        * Maximal reconnection delay
        */
      val MAX_RECONNECTION_DELAY_MS = "metadata.cluster.max-reconnection-delay-ms"

      /**
        * Query retry count
        */
      val QUERY_RETRY_COUNT = "metadata.cluster.query-retry-count"

      /**
        * Connection timeout
        */
      val CONNECTION_TIMEOUT_MS = "metadata.cluster.connection-timeout-ms"

      /**
        * Read timeout
        */
      val READ_TIMEOUT_MS = "metadata.cluster.read-timeout-ms"

    }

  }

  /**
    * TSF_Dictionary data scope
    */
  object Data {

    /**
      * TSF_Dictionary cluster scope
      */
    object Cluster {
      /**
        * the database driver which is used for data storage (currently UniversalFactory supports aerospike
        */
      val DRIVER = "data.cluster.driver"
      /**
        * the name of akka actor system used with t-streams
        */
      val ENDPOINTS = "data.cluster.endpoints"
      /**
        * endpoints list of the database where txn data is stored, comma separated: host1:port1,host2:port2,host3:port3,...
        */
      val NAMESPACE = "data.cluster.namespace"
      /**
        * login name of the user which can access the data storage
        */
      val LOGIN = "data.cluster.login"
      /**
        * password of the user which can access the data storage
        */
      val PASSWORD = "data.cluster.password"

      /**
        * Stores consts for TSF_Dictionary.Data.Cluster
        */
      object Consts {
        /**
          * Definition for aerospike backend
          */
        val DATA_DRIVER_AEROSPIKE = "aerospike"
        /**
          * Definition for cassandra backend
          */
        val DATA_DRIVER_CASSANDRA = "cassandra"

        /**
          * Definition for hazelcast backend
          */
        val DATA_DRIVER_HAZELCAST = "hazelcast"
      }

      /**
        * scope TSF_Dictionary.Data.Cluster hazelcast engine
        */
      object Hazelcast {
        /**
          * Amount of synchronous replicas
          */
        val SYNCHRONOUS_REPLICAS = "data.cluster.hazelcast.synchronous-replicas"
        /**
          * Amount of asynchronous replicas
          */
        val ASYNCHRONOUS_REPLICAS = "data.cluster.hazelcast.asynchronous-replicas"
      }
      
      /**
        * scope TSF_Dictionary.Data.Cluster aerospike engine
        */
      object Aerospike {
        /**
          * user can specify write policy if required
          */
        val WRITE_POLICY = "data.cluster.aerospike.write-policy"
        /**
          * user can specify read policy if required
          */
        val READ_POLICY = "data.cluster.aerospike.read-policy"
        /**
          * user can specify client policy
          */
        val CLIENT_POLICY = "data.cluster.aerospike.client-policy"

      }

      object Cassandra {
        /**
          * Local DC
          */
        val LOCAL_DC = "data.cluster.casandra.local-dc"

        /**
          * Connection keep alive in MS
          */
        val KEEP_ALIVE_MS = "data.cluster.casandra.keep-alive-ms"

        /**
          * Minimal reconnection delay
          */
        val MIN_RECONNECTION_DELAY_MS = "data.cluster.casandra.min-reconnection-delay-ms"

        /**
          * Maximal reconnection delay
          */
        val MAX_RECONNECTION_DELAY_MS = "data.cluster.casandra.max-reconnection-delay-ms"

        /**
          * Query retry count
          */
        val QUERY_RETRY_COUNT = "data.cluster.casandra.query-retry-count"

        /**
          * Connection timeout
          */
        val CONNECTION_TIMEOUT_MS = "data.cluster.casandra.connection-timeout-ms"

        /**
          * Read timeout
          */
        val READ_TIMEOUT_MS = "data.cluster.casandra.read-timeout-ms"
      }

    }

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


    //TODO: fix internals transport->master


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
        * amount of data items to batch when write data into txn
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
      * name of consumer
      */
    val NAME = "producer.name"

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
      val THREAD_POOL = "consumer.subscriber.thread-pool"

      /**
        * thread pool size
        */
      val POLLING_FREQUENCY_DELAY = "consumer.subscriber.polling-frequency-delay"

    }

  }

}

/**
  * Created by ivan on 21.07.16.
  */
class TStreamsFactory(envname: String = "T-streams") {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val lck = new ReentrantLock(true)
  val propertyMap = new HashMap[String, Any]()
  val isClosed = new AtomicBoolean(false)
  val isLocked = new AtomicBoolean(false)

  // metadata cluster scope
  propertyMap += (TSF_Dictionary.Metadata.Cluster.ENDPOINTS -> "localhost:9042")
  propertyMap += (TSF_Dictionary.Metadata.Cluster.NAMESPACE -> "test")
  propertyMap += (TSF_Dictionary.Metadata.Cluster.LOGIN -> null)
  propertyMap += (TSF_Dictionary.Metadata.Cluster.PASSWORD -> null)

  // data cluster scope
  propertyMap += (TSF_Dictionary.Data.Cluster.DRIVER -> TSF_Dictionary.Data.Cluster.Consts.DATA_DRIVER_AEROSPIKE)
  propertyMap += (TSF_Dictionary.Data.Cluster.ENDPOINTS -> "localhost:3000")
  propertyMap += (TSF_Dictionary.Data.Cluster.NAMESPACE -> "test")
  propertyMap += (TSF_Dictionary.Data.Cluster.LOGIN -> null)
  propertyMap += (TSF_Dictionary.Data.Cluster.PASSWORD -> null)
  propertyMap += (TSF_Dictionary.Data.Cluster.Aerospike.WRITE_POLICY -> null)
  propertyMap += (TSF_Dictionary.Data.Cluster.Aerospike.READ_POLICY -> null)
  propertyMap += (TSF_Dictionary.Data.Cluster.Aerospike.CLIENT_POLICY -> null)
  val Cluster_cassandra_keep_alive_ms_default = 5000
  val Cluster_cassandra_keep_alive_ms_min = 1000
  val Cluster_cassandra_keep_alive_ms_max = 10000
  val Cluster_cassandra_min_reconnection_delay_ms_default = 1000
  val Cluster_cassandra_min_reconnection_delay_ms_min = 500
  val Cluster_cassandra_min_reconnection_delay_ms_max = 5000
  val Cluster_cassandra_max_reconnection_delay_ms_default = 60000
  val Cluster_cassandra_max_reconnection_delay_ms_min = 5000
  val Cluster_cassandra_max_reconnection_delay_ms_max = 60000
  val Cluster_cassandra_query_retry_count_default = 10
  val Cluster_cassandra_query_retry_count_min = 1
  val Cluster_cassandra_query_retry_count_max = 20
  val Cluster_cassandra_connection_timeout_ms_default = 5000
  val Cluster_cassandra_connection_timeout_ms_min = 1000
  val Cluster_cassandra_connection_timeout_ms_max = 60000
  val Cluster_cassandra_read_timeout_ms_default = 120000
  val Cluster_cassandra_read_timeout_ms_min = 10000
  val Cluster_cassandra_read_timeout_ms_max = 600000

  propertyMap += (TSF_Dictionary.Metadata.Cluster.LOCAL_DC                        -> null)
  propertyMap += (TSF_Dictionary.Metadata.Cluster.KEEP_ALIVE_MS                   -> Cluster_cassandra_keep_alive_ms_default)
  propertyMap += (TSF_Dictionary.Metadata.Cluster.MIN_RECONNECTION_DELAY_MS       -> Cluster_cassandra_min_reconnection_delay_ms_default)
  propertyMap += (TSF_Dictionary.Metadata.Cluster.MAX_RECONNECTION_DELAY_MS       -> Cluster_cassandra_max_reconnection_delay_ms_default)
  propertyMap += (TSF_Dictionary.Metadata.Cluster.QUERY_RETRY_COUNT               -> Cluster_cassandra_query_retry_count_default)
  propertyMap += (TSF_Dictionary.Metadata.Cluster.CONNECTION_TIMEOUT_MS           -> Cluster_cassandra_connection_timeout_ms_default)
  propertyMap += (TSF_Dictionary.Metadata.Cluster.READ_TIMEOUT_MS                 -> Cluster_cassandra_read_timeout_ms_default)

  propertyMap += (TSF_Dictionary.Data.Cluster.Cassandra.LOCAL_DC                  -> null)
  propertyMap += (TSF_Dictionary.Data.Cluster.Cassandra.KEEP_ALIVE_MS             -> Cluster_cassandra_keep_alive_ms_default)
  propertyMap += (TSF_Dictionary.Data.Cluster.Cassandra.MIN_RECONNECTION_DELAY_MS -> Cluster_cassandra_min_reconnection_delay_ms_default)
  propertyMap += (TSF_Dictionary.Data.Cluster.Cassandra.MAX_RECONNECTION_DELAY_MS -> Cluster_cassandra_max_reconnection_delay_ms_default)
  propertyMap += (TSF_Dictionary.Data.Cluster.Cassandra.QUERY_RETRY_COUNT         -> Cluster_cassandra_query_retry_count_default)
  propertyMap += (TSF_Dictionary.Data.Cluster.Cassandra.CONNECTION_TIMEOUT_MS     -> Cluster_cassandra_connection_timeout_ms_default)
  propertyMap += (TSF_Dictionary.Data.Cluster.Cassandra.READ_TIMEOUT_MS           -> Cluster_cassandra_read_timeout_ms_default)

  val Cluster_hazelcast_synchronous_replicas_default = 0
  val Cluster_hazelcast_asynchronous_replicas_default = 0
  propertyMap += (TSF_Dictionary.Data.Cluster.Hazelcast.SYNCHRONOUS_REPLICAS -> Cluster_hazelcast_synchronous_replicas_default)
  propertyMap += (TSF_Dictionary.Data.Cluster.Hazelcast.ASYNCHRONOUS_REPLICAS -> Cluster_hazelcast_asynchronous_replicas_default)

  // coordination scope
  propertyMap += (TSF_Dictionary.Coordination.ENDPOINTS -> "localhost:2181")
  propertyMap += (TSF_Dictionary.Coordination.ROOT -> "/t-streams")

  val Coordination_ttl_default = 7
  val Coordination_ttl_min = 1
  val Coordination_ttl_max = 30
  propertyMap += (TSF_Dictionary.Coordination.TTL -> Coordination_ttl_default)

  val Coordination_connection_timeout_default = 7
  val Coordination_connection_timeout_min = 1
  val Coordination_connection_timeout_max = 30
  propertyMap += (TSF_Dictionary.Coordination.CONNECTION_TIMEOUT -> Coordination_connection_timeout_default)

  // stream scope
  propertyMap += (TSF_Dictionary.Stream.NAME -> "test")

  val Stream_partitions_default = 1
  val Stream_partitions_min = 1
  val Stream_partitions_max = 100000000
  propertyMap += (TSF_Dictionary.Stream.PARTITIONS -> Stream_partitions_default)

  val Stream_ttl_default = 60 * 60 * 24
  val Stream_ttl_min = 60
  val Stream_ttl_max = 315360000
  propertyMap += (TSF_Dictionary.Stream.TTL -> Stream_ttl_default)
  propertyMap += (TSF_Dictionary.Stream.DESCRIPTION -> "Test stream")

  // producer scope
  propertyMap += (TSF_Dictionary.Producer.BIND_HOST -> "localhost")
  propertyMap += (TSF_Dictionary.Producer.BIND_PORT -> 18000)
  val Producer_transport_timeout_default = 5
  val Producer_transport_timeout_min = 1
  val Producer_transport_timeout_max = 10
  propertyMap += (TSF_Dictionary.Producer.TRANSPORT_TIMEOUT -> Producer_transport_timeout_default)

  val Producer_transport_retry_count_default = 3
  val Producer_transport_retry_delay_default = 5

  propertyMap += (TSF_Dictionary.Producer.TRANSPORT_RETRY_COUNT -> Producer_transport_retry_count_default)
  propertyMap += (TSF_Dictionary.Producer.TRANSPORT_RETRY_DELAY -> Producer_transport_retry_delay_default)

  val Producer_transaction_ttl_default = 6
  val Producer_transaction_ttl_min = 3
  val Producer_transaction_ttl_max = 15
  propertyMap += (TSF_Dictionary.Producer.Transaction.TTL -> Producer_transaction_ttl_default)

  val Producer_transaction_open_maxwait_default = 5
  val Producer_transaction_open_maxwait_min = 1
  val Producer_transaction_open_maxwait_max = 10
  propertyMap += (TSF_Dictionary.Producer.Transaction.OPEN_MAXWAIT -> Producer_transaction_open_maxwait_default)

  val Producer_transaction_keep_alive_default = 1
  val Producer_transaction_keep_alive_min = 1
  val Producer_transaction_keep_alive_max = 2
  propertyMap += (TSF_Dictionary.Producer.Transaction.KEEP_ALIVE -> Producer_transaction_keep_alive_default)

  val Producer_transaction_data_write_batch_size_default = 100
  val Producer_transaction_data_write_batch_size_min = 1
  val Producer_transaction_data_write_batch_size_max = 1000
  propertyMap += (TSF_Dictionary.Producer.Transaction.DATA_WRITE_BATCH_SIZE -> Producer_transaction_data_write_batch_size_default)
  propertyMap += (TSF_Dictionary.Producer.Transaction.DISTRIBUTION_POLICY -> TSF_Dictionary.Producer.Transaction.Consts.DISTRIBUTION_POLICY_RR)

  val Producer_thread_pool_default = 4
  val Producer_thread_pool_min = 1
  val Producer_thread_pool_max = 64
  propertyMap += (TSF_Dictionary.Producer.THREAD_POOL -> Producer_thread_pool_default)

  // consumer scope
  val Consumer_transaction_preload_default = 10
  val Consumer_transaction_preload_min = 1
  val Consumer_transaction_preload_max = 100
  propertyMap += (TSF_Dictionary.Consumer.TRANSACTION_PRELOAD -> Consumer_transaction_preload_default)
  val Consumer_data_preload_default = 100
  val Consumer_data_preload_min = 10
  val Consumer_data_preload_max = 200
  propertyMap += (TSF_Dictionary.Consumer.DATA_PRELOAD -> Consumer_data_preload_default)
  propertyMap += (TSF_Dictionary.Consumer.Subscriber.BIND_HOST -> "localhost")
  propertyMap += (TSF_Dictionary.Consumer.Subscriber.BIND_PORT -> 18001)
  propertyMap += (TSF_Dictionary.Consumer.Subscriber.PERSISTENT_QUEUE_PATH -> "/tmp")

  val Subscriber_thread_pool_default = 4
  val Subscriber_thread_pool_min = 1
  val Subscriber_thread_pool_max = 64
  propertyMap += (TSF_Dictionary.Consumer.Subscriber.THREAD_POOL -> Subscriber_thread_pool_default)

  val Subscriber_polling_frequency_delay_default = 100
  val Subscriber_polling_frequency_delay_min = 1
  val Subscriber_polling_frequency_delay_max = 1000
  propertyMap += (TSF_Dictionary.Consumer.Subscriber.POLLING_FREQUENCY_DELAY -> Subscriber_polling_frequency_delay_default)


  //metadata/data factories
  val msFactory = new MetadataStorageFactory

  /**
    * locks factory, after lock setProperty leads to exception.
    */
  def lock():Unit = isLocked.set(true)

  /**
    * clones factory
    */
  def copy(): TStreamsFactory = {
    if (isClosed.get)
      throw new IllegalStateException("TStreamsFactory is closed. This is the illegal usage of the object.")

    LockUtil.withLockOrDieDo[TStreamsFactory](lck, (100, TimeUnit.SECONDS), Some(logger), () => {
      val f = new TStreamsFactory()
      propertyMap.foreach((kv) => f.setProperty(kv._1,kv._2))
      f
    })
  }
  /**
    *
    * @param key
    * @param value
    * @return
    */
  def setProperty(key: String, value: Any): TStreamsFactory = {
    if (isClosed.get)
      throw new IllegalStateException("TStreamsFactory is closed. This is the illegal usage of the object.")

    if(isLocked.get)
      throw new IllegalStateException("TStreamsFactory is locked. Use clone() to set properties.")

    LockUtil.withLockOrDieDo[TStreamsFactory](lck, (100, TimeUnit.SECONDS), Some(logger), () => {
      logger.debug("set property " + key + " = " + value)
      if (propertyMap contains key)
        propertyMap += (key -> value)
      else
        throw new IllegalArgumentException("Property " + key + " is unknown and can not be altered.")
      this
    })
  }

  /**
    *
    * @param key
    * @return
    */
  def getProperty(key: String): Any = {
    if (isClosed.get)
      throw new IllegalStateException("TStreamsFactory is closed. This is the illegal usage of the object.")

    LockUtil.withLockOrDieDo[Any](lck, (100, TimeUnit.SECONDS), Some(logger), () => {
      val v = propertyMap get key
      logger.debug("get property " + key + " = " + v.getOrElse(null))
      v.getOrElse(null)
    })
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
    * common routine allows getting ready to use data store object
    *
    * @return
    */
  def getDataStorage(): IStorage[Array[Byte]] = {
    if (pAsString(TSF_Dictionary.Data.Cluster.DRIVER) == TSF_Dictionary.Data.Cluster.Consts.DATA_DRIVER_AEROSPIKE) {
      val dsf = new com.bwsw.tstreams.data.aerospike.Factory

      // construct client policy
      var cp: ClientPolicy = null
      if (getProperty(TSF_Dictionary.Data.Cluster.Aerospike.CLIENT_POLICY) == null)
        cp = new ClientPolicy()
      else
        cp = getProperty(TSF_Dictionary.Data.Cluster.Aerospike.CLIENT_POLICY).asInstanceOf[ClientPolicy]

      cp.user = pAsString(TSF_Dictionary.Data.Cluster.LOGIN, null)
      cp.password = pAsString(TSF_Dictionary.Data.Cluster.PASSWORD, null)

      // construct write policy
      var wp: WritePolicy = null
      if (getProperty(TSF_Dictionary.Data.Cluster.Aerospike.WRITE_POLICY) == null)
        wp = new WritePolicy()
      else
        wp = getProperty(TSF_Dictionary.Data.Cluster.Aerospike.WRITE_POLICY).asInstanceOf[WritePolicy]

      // construct write policy
      var rp: Policy = null
      if (getProperty(TSF_Dictionary.Data.Cluster.Aerospike.READ_POLICY) == null)
        rp = new WritePolicy()
      else
        rp = getProperty(TSF_Dictionary.Data.Cluster.Aerospike.READ_POLICY).asInstanceOf[Policy]

      val namespace = pAsString(TSF_Dictionary.Data.Cluster.NAMESPACE)
      val data_cluster_endpoints = pAsString(TSF_Dictionary.Data.Cluster.ENDPOINTS)
      assert(namespace != null)
      assert(data_cluster_endpoints != null)

      val opts = new com.bwsw.tstreams.data.aerospike.Options(
        namespace = namespace,
        hosts = scala.Predef.Set.empty[Host] ++ NetworkUtil.getAerospikeCompatibleHostList(data_cluster_endpoints),
        clientPolicy = cp,
        writePolicy = wp,
        readPolicy = rp)

      // create instance of aerospike data storage
      return dsf.getInstance(opts)

    }
    else if (pAsString(TSF_Dictionary.Data.Cluster.DRIVER) == TSF_Dictionary.Data.Cluster.Consts.DATA_DRIVER_CASSANDRA) {
      val dsf = new com.bwsw.tstreams.data.cassandra.Factory

      val login = pAsString(TSF_Dictionary.Data.Cluster.LOGIN, null)
      val password = pAsString(TSF_Dictionary.Data.Cluster.PASSWORD, null)

      assert(pAsString(TSF_Dictionary.Data.Cluster.NAMESPACE) != null)
      assert(pAsString(TSF_Dictionary.Data.Cluster.ENDPOINTS) != null)

      val opts = new CassandraConnectorConf(
        hosts     = scala.Predef.Set.empty[InetSocketAddress] ++ NetworkUtil.getInetSocketAddressCompatibleHostList(pAsString(TSF_Dictionary.Data.Cluster.ENDPOINTS)),
        login     = login,
        password  = password,
        localDC   = Option(pAsString(TSF_Dictionary.Data.Cluster.Cassandra.LOCAL_DC, null)),
        keepAliveMillis             = pAsInt(TSF_Dictionary.Data.Cluster.Cassandra.KEEP_ALIVE_MS, Cluster_cassandra_keep_alive_ms_default),
        minReconnectionDelayMillis  = pAsInt(TSF_Dictionary.Data.Cluster.Cassandra.MIN_RECONNECTION_DELAY_MS, Cluster_cassandra_min_reconnection_delay_ms_default),
        maxReconnectionDelayMillis  = pAsInt(TSF_Dictionary.Data.Cluster.Cassandra.MAX_RECONNECTION_DELAY_MS, Cluster_cassandra_max_reconnection_delay_ms_default),
        queryRetryCount             = pAsInt(TSF_Dictionary.Data.Cluster.Cassandra.QUERY_RETRY_COUNT, Cluster_cassandra_query_retry_count_default),
        connectTimeoutMillis        = pAsInt(TSF_Dictionary.Data.Cluster.Cassandra.CONNECTION_TIMEOUT_MS, Cluster_cassandra_connection_timeout_ms_default),
        readTimeoutMillis           = pAsInt(TSF_Dictionary.Data.Cluster.Cassandra.READ_TIMEOUT_MS, Cluster_cassandra_read_timeout_ms_default)
      )

      return dsf.getInstance(opts, pAsString(TSF_Dictionary.Data.Cluster.NAMESPACE))
    }
    else if (pAsString(TSF_Dictionary.Data.Cluster.DRIVER) == TSF_Dictionary.Data.Cluster.Consts.DATA_DRIVER_HAZELCAST) {
      val dsf = new com.bwsw.tstreams.data.hazelcast.Factory()
      val opts = com.bwsw.tstreams.data.hazelcast.Options(
        pAsString(TSF_Dictionary.Data.Cluster.NAMESPACE),
        pAsInt(pAsString(TSF_Dictionary.Data.Cluster.Hazelcast.SYNCHRONOUS_REPLICAS)),
        pAsInt(pAsString(TSF_Dictionary.Data.Cluster.Hazelcast.ASYNCHRONOUS_REPLICAS)))
      return dsf.getInstance(opts)
    }
    else {
      throw new InvalidParameterException("Only TSF_Dictionary.Data.Cluster.Consts.DATA_DRIVER_CASSANDRA, TSF_Dictionary.Data.Cluster.Consts.DATA_DRIVER_HAZELCAST and " +
        "TSF_Dictionary.Data.Cluster.Consts.DATA_DRIVER_AEROSPIKE engines " +
        "are supported currently in UniversalFactory. Received: " + pAsString(TSF_Dictionary.Data.Cluster.DRIVER))
    }
    null
  }

  /**
    * common routine which allows getting ready to use metadata object
    *
    * @return
    */
  def getMetadataStorage(): MetadataStorage = {
    val login = pAsString(TSF_Dictionary.Metadata.Cluster.LOGIN, null)
    val password = pAsString(TSF_Dictionary.Metadata.Cluster.PASSWORD, null)

    assert(pAsString(TSF_Dictionary.Metadata.Cluster.NAMESPACE) != null)
    assert(pAsString(TSF_Dictionary.Metadata.Cluster.ENDPOINTS) != null)

    val opts = new CassandraConnectorConf(
      hosts     = scala.Predef.Set.empty[InetSocketAddress] ++ NetworkUtil.getInetSocketAddressCompatibleHostList(pAsString(TSF_Dictionary.Metadata.Cluster.ENDPOINTS)),
      login     = login,
      password  = password,
      localDC   = Option(pAsString(TSF_Dictionary.Metadata.Cluster.LOCAL_DC, null)),
      keepAliveMillis             = pAsInt(TSF_Dictionary.Metadata.Cluster.KEEP_ALIVE_MS, Cluster_cassandra_keep_alive_ms_default),
      minReconnectionDelayMillis  = pAsInt(TSF_Dictionary.Metadata.Cluster.MIN_RECONNECTION_DELAY_MS, Cluster_cassandra_min_reconnection_delay_ms_default),
      maxReconnectionDelayMillis  = pAsInt(TSF_Dictionary.Metadata.Cluster.MAX_RECONNECTION_DELAY_MS, Cluster_cassandra_max_reconnection_delay_ms_default),
      queryRetryCount             = pAsInt(TSF_Dictionary.Metadata.Cluster.QUERY_RETRY_COUNT, Cluster_cassandra_query_retry_count_default),
      connectTimeoutMillis        = pAsInt(TSF_Dictionary.Metadata.Cluster.CONNECTION_TIMEOUT_MS, Cluster_cassandra_connection_timeout_ms_default),
      readTimeoutMillis           = pAsInt(TSF_Dictionary.Metadata.Cluster.READ_TIMEOUT_MS, Cluster_cassandra_read_timeout_ms_default)
    )

    // construct metadata storage
    return msFactory.getInstance(opts,pAsString(TSF_Dictionary.Metadata.Cluster.NAMESPACE))
  }

  /**
    * common routine which allows to get ready to use stream object by env
    *
    * @param metadatastorage
    * @param datastorage
    * @return
    */
  private def getStreamObject(metadatastorage: MetadataStorage, datastorage: IStorage[Array[Byte]]): TStream[Array[Byte]] = {
    assert(pAsString(TSF_Dictionary.Stream.NAME) != null)
    pAssertIntRange(pAsInt(TSF_Dictionary.Stream.PARTITIONS, Stream_partitions_default), Stream_partitions_min, Stream_partitions_max)
    pAssertIntRange(pAsInt(TSF_Dictionary.Stream.TTL, Stream_ttl_default), Stream_ttl_min, Stream_ttl_max)

    // construct stream
    val stream = new TStream[Array[Byte]](
      name = pAsString(TSF_Dictionary.Stream.NAME),
      partitions = pAsInt(TSF_Dictionary.Stream.PARTITIONS, Stream_partitions_default),
      metadataStorage = metadatastorage,
      dataStorage = datastorage,
      ttl = pAsInt(TSF_Dictionary.Stream.TTL, Stream_ttl_default),
      description = pAsString(TSF_Dictionary.Stream.DESCRIPTION, ""))
    return stream
  }

  /**
    * reusable method which returns consumer options object
    */
  private def getBasicConsumerOptions[USERTYPE](stream: TStream[Array[Byte]],
                                                partitions: List[Int],
                                                converter: IConverter[Array[Byte], USERTYPE],
                                                txnGenerator: IUUIDGenerator,
                                                offset: IOffset,
                                                isUseLastOffset: Boolean = true): com.bwsw.tstreams.agents.consumer.Options[USERTYPE] = {
    val consumer_transaction_preload = pAsInt(TSF_Dictionary.Consumer.TRANSACTION_PRELOAD, Consumer_transaction_preload_default)
    pAssertIntRange(consumer_transaction_preload, Consumer_transaction_preload_min, Consumer_transaction_preload_max)

    val consumer_data_preload = pAsInt(TSF_Dictionary.Consumer.DATA_PRELOAD, Consumer_data_preload_default)
    pAssertIntRange(consumer_data_preload, Consumer_data_preload_min, Consumer_data_preload_max)

    val consumerOptions = new com.bwsw.tstreams.agents.consumer.Options[USERTYPE](transactionsPreload = consumer_transaction_preload, dataPreload = consumer_data_preload, converter = converter, readPolicy = new RoundRobinPolicy(stream, partitions), offset = offset, txnGenerator = txnGenerator, useLastOffset = isUseLastOffset)

    consumerOptions
  }

  /**
    * return returns basic scream object
    */
  def getStream(): TStream[Array[Byte]] = {
    if (isClosed.get)
      throw new IllegalStateException("TStreamsFactory is closed. This is the illegal usage of the object.")

    LockUtil.withLockOrDieDo[TStream[Array[Byte]]](lck, (100, TimeUnit.SECONDS), Some(logger), () => {
      val ds: IStorage[Array[Byte]] = getDataStorage()
      val ms: MetadataStorage = getMetadataStorage()
      val stream: TStream[Array[Byte]] = getStreamObject(metadatastorage = ms, datastorage = ds)
      stream
    })

  }

  /**
    * returns ready to use producer object
    *
    * @param name Producer name
    * @param isLowPriority
    * @param txnGenerator
    * @param converter
    * @param partitions
    * @tparam USERTYPE - type convert data from
    * @return
    */
  def getProducer[USERTYPE](name: String,
                            txnGenerator: IUUIDGenerator,
                            converter: IConverter[USERTYPE, Array[Byte]],
                            partitions: List[Int],
                            isLowPriority: Boolean = false
                           ): Producer[USERTYPE] = {

    if (isClosed.get)
      throw new IllegalStateException("TStreamsFactory is closed. This is the illegal usage of the object.")

    LockUtil.withLockOrDieDo[Producer[USERTYPE]](lck, (100, TimeUnit.SECONDS), Some(logger), () => {

      val ds: IStorage[Array[Byte]] = getDataStorage()
      val ms: MetadataStorage = getMetadataStorage()
      val stream: TStream[Array[Byte]] = getStreamObject(metadatastorage = ms, datastorage = ds)

      assert(pAsString(TSF_Dictionary.Producer.BIND_PORT) != null)
      assert(pAsString(TSF_Dictionary.Producer.BIND_HOST) != null)
      assert(pAsString(TSF_Dictionary.Coordination.ENDPOINTS) != null)
      assert(pAsString(TSF_Dictionary.Coordination.ROOT) != null)

      pAssertIntRange(pAsInt(TSF_Dictionary.Coordination.TTL, Coordination_ttl_default), Coordination_ttl_min, Coordination_ttl_max)
      pAssertIntRange(pAsInt(TSF_Dictionary.Producer.TRANSPORT_TIMEOUT, Producer_transport_timeout_default), Producer_transport_timeout_min, Producer_transport_timeout_max)
      pAssertIntRange(pAsInt(TSF_Dictionary.Coordination.CONNECTION_TIMEOUT, Coordination_connection_timeout_default),
        Coordination_connection_timeout_min, Coordination_connection_timeout_max)

      val transport = new TcpTransport(
        pAsString(TSF_Dictionary.Producer.BIND_HOST) + ":" + pAsString(TSF_Dictionary.Producer.BIND_PORT),
        pAsInt(TSF_Dictionary.Producer.TRANSPORT_TIMEOUT, Producer_transport_timeout_default) * 1000,
        pAsInt(TSF_Dictionary.Producer.TRANSPORT_RETRY_COUNT, Producer_transport_retry_count_default),
        pAsInt(TSF_Dictionary.Producer.TRANSPORT_RETRY_DELAY, Producer_transport_retry_delay_default) * 1000)

      val cao = new CoordinationOptions(zkHosts = NetworkUtil.getInetSocketAddressCompatibleHostList(pAsString(TSF_Dictionary.Coordination.ENDPOINTS)), zkRootPath = pAsString(TSF_Dictionary.Coordination.ROOT), zkSessionTimeout = pAsInt(TSF_Dictionary.Coordination.TTL, Coordination_ttl_default), zkConnectionTimeout = pAsInt(TSF_Dictionary.Coordination.CONNECTION_TIMEOUT, Coordination_connection_timeout_default), isLowPriorityToBeMaster = isLowPriority, transport = transport)


      var writePolicy: AbstractPolicy = null

      if (pAsString(TSF_Dictionary.Producer.Transaction.DISTRIBUTION_POLICY) ==
        TSF_Dictionary.Producer.Transaction.Consts.DISTRIBUTION_POLICY_RR) {
        writePolicy = new RoundRobinPolicy(stream, partitions)
      }
      else {
        throw new InvalidParameterException("Only TSF_Dictionary.Producer.Transaction.Consts.DISTRIBUTION_POLICY_RR policy " +
          "is supported currently in UniversalFactory.")
      }

      pAssertIntRange(pAsInt(TSF_Dictionary.Producer.Transaction.TTL, Producer_transaction_ttl_default), Producer_transaction_ttl_min, Producer_transaction_ttl_max)
      pAssertIntRange(pAsInt(TSF_Dictionary.Producer.Transaction.KEEP_ALIVE, Producer_transaction_keep_alive_default), Producer_transaction_keep_alive_min, Producer_transaction_keep_alive_max)
      assert(pAsInt(TSF_Dictionary.Producer.Transaction.TTL, Producer_transaction_ttl_default) >=
        pAsInt(TSF_Dictionary.Producer.Transaction.KEEP_ALIVE, Producer_transaction_keep_alive_default) * 3)

      val insertCnt = pAsInt(TSF_Dictionary.Producer.Transaction.DATA_WRITE_BATCH_SIZE, Producer_transaction_data_write_batch_size_default)
      pAssertIntRange(insertCnt,
        Producer_transaction_data_write_batch_size_min, Producer_transaction_data_write_batch_size_max)

      val po = new com.bwsw.tstreams.agents.producer.Options[USERTYPE](transactionTTL = pAsInt(TSF_Dictionary.Producer.Transaction.TTL, Producer_transaction_ttl_default), transactionKeepAliveInterval = pAsInt(TSF_Dictionary.Producer.Transaction.KEEP_ALIVE, Producer_transaction_keep_alive_default), writePolicy = writePolicy, batchSize = insertCnt, txnGenerator = txnGenerator, coordinationOptions = cao, converter = converter)

      new Producer[USERTYPE](name = name, stream = stream, producerOptions = po)
    })
  }

  /**
    * returns ready to use consumer object
    *
    * @param name Consumer name
    * @param txnGenerator
    * @param converter
    * @param partitions
    * @tparam USERTYPE type to convert data to
    * @return
    */
  def getConsumer[USERTYPE](name: String,
                            txnGenerator: IUUIDGenerator,
                            converter: IConverter[Array[Byte], USERTYPE],
                            partitions: List[Int],
                            offset: IOffset,
                            isUseLastOffset: Boolean = true
                           ): Consumer[USERTYPE] = {

    if (isClosed.get)
      throw new IllegalStateException("TStreamsFactory is closed. This is the illegal usage of the object.")

    LockUtil.withLockOrDieDo[Consumer[USERTYPE]](lck, (100, TimeUnit.SECONDS), Some(logger), () => {

      val ds: IStorage[Array[Byte]] = getDataStorage()
      val ms: MetadataStorage = getMetadataStorage()
      val stream: TStream[Array[Byte]] = getStreamObject(metadatastorage = ms, datastorage = ds)
      val consumerOptions = getBasicConsumerOptions(txnGenerator = txnGenerator,
        stream = stream,
        partitions = partitions,
        converter = converter,
        offset = offset,
        isUseLastOffset = isUseLastOffset)

      new Consumer(name, stream, consumerOptions)
    })
  }

  /**
    * returns ready to use subscribing consumer object
    *
    * @param txnGenerator
    * @param converter
    * @param partitions
    * @param callback
    * @tparam USERTYPE - type to convert data to
    * @return
    */
  def getSubscriber[USERTYPE](name: String,
                              txnGenerator: IUUIDGenerator,
                              converter: IConverter[Array[Byte], USERTYPE],
                              partitions: List[Int],
                              callback: Callback[USERTYPE],
                              offset: IOffset,
                              isUseLastOffset: Boolean = true
                             ): SubscribingConsumer[USERTYPE] = {
    if (isClosed.get)
      throw new IllegalStateException("TStreamsFactory is closed. This is the illegal usage of the object.")

    LockUtil.withLockOrDieDo[SubscribingConsumer[USERTYPE]](lck, (100, TimeUnit.SECONDS), Some(logger), () => {

      val ds: IStorage[Array[Byte]] = getDataStorage()
      val ms: MetadataStorage = getMetadataStorage()
      val stream: TStream[Array[Byte]] = getStreamObject(metadatastorage = ms, datastorage = ds)

      val consumerOptions = getBasicConsumerOptions(txnGenerator = txnGenerator,
        stream = stream,
        partitions = partitions,
        converter = converter,
        offset = offset,
        isUseLastOffset = isUseLastOffset)

      //val consumer = new BasicConsumer(name, stream, consumerOptions)
      val bind_host = pAsString(TSF_Dictionary.Consumer.Subscriber.BIND_HOST)
      assert(bind_host != null)
      val bind_port = pAsString(TSF_Dictionary.Consumer.Subscriber.BIND_PORT)
      assert(bind_port != null)
      val endpoints = pAsString(TSF_Dictionary.Coordination.ENDPOINTS)
      assert(endpoints != null)
      val root = pAsString(TSF_Dictionary.Coordination.ROOT)
      assert(root != null)

      val ttl = pAsInt(TSF_Dictionary.Coordination.TTL, Coordination_ttl_default)
      pAssertIntRange(ttl, Coordination_ttl_min, Coordination_ttl_max)
      val conn_timeout = pAsInt(TSF_Dictionary.Coordination.CONNECTION_TIMEOUT, Coordination_connection_timeout_default)
      pAssertIntRange(conn_timeout,
        Coordination_connection_timeout_min, Coordination_connection_timeout_max)

      val thread_pool = pAsInt(TSF_Dictionary.Consumer.Subscriber.THREAD_POOL, Subscriber_thread_pool_default)
      pAssertIntRange(thread_pool,
        Subscriber_thread_pool_min, Subscriber_thread_pool_max)

      val polling_frequency = pAsInt(TSF_Dictionary.Consumer.Subscriber.POLLING_FREQUENCY_DELAY, Subscriber_polling_frequency_delay_default)
      pAssertIntRange(polling_frequency,
        Subscriber_polling_frequency_delay_min, Subscriber_polling_frequency_delay_max)


      val coordinationOptions = new SubscriberCoordinationOptions(
        agentAddress = bind_host + ":" + bind_port,
        zkRootPath = root,
        zkHosts = NetworkUtil.getInetSocketAddressCompatibleHostList(endpoints),
        zkSessionTimeout = ttl,
        zkConnectionTimeout = conn_timeout,
        threadPoolAmount = thread_pool)

      val queue_path = pAsString(TSF_Dictionary.Consumer.Subscriber.PERSISTENT_QUEUE_PATH)
      assert(queue_path != null)

      new SubscribingConsumer[USERTYPE](
        name = name,
        stream = stream,
        options = consumerOptions,
        subscriberCoordinationOptions = coordinationOptions,
        callBack = callback,
        persistentQueuePath = queue_path,
        pollingFrequencyMaxDelay = polling_frequency)

    })
  }

  /**
    * closes t-streams factory and stops further object creation
    */
  def close(): Unit = {
    if (isClosed.getAndSet(true))
      throw new IllegalStateException("TStreamsFactory is closed. This is repeatable close operation.")

    msFactory.closeFactory()
    // TODO FIXIT
    //aerospikeStorageFactory.closeFactory()
  }

}
