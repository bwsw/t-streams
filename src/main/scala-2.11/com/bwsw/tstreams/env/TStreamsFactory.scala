package com.bwsw.tstreams.env

import java.net.InetSocketAddress
import java.security.InvalidParameterException
import java.util.concurrent.locks.ReentrantLock

import akka.actor.ActorSystem
import com.aerospike.client.Host
import com.aerospike.client.policy.{ClientPolicy, Policy, WritePolicy}
import com.bwsw.tstreams.agents.consumer.Offsets.{IOffset, Oldest}
import com.bwsw.tstreams.agents.consumer.{BasicConsumer, BasicConsumerOptions, SubscriberCoordinationOptions}
import com.bwsw.tstreams.agents.consumer.subscriber.{BasicSubscriberCallback, BasicSubscribingConsumer}
import com.bwsw.tstreams.agents.producer.InsertionType.{BatchInsert, InsertType, SingleElementInsert}
import com.bwsw.tstreams.agents.producer.{BasicProducer, BasicProducerOptions, ProducerCoordinationOptions}
import com.bwsw.tstreams.converter.IConverter
import com.bwsw.tstreams.coordination.transactions.transport.impl.TcpTransport
import com.bwsw.tstreams.data.IStorage
import com.bwsw.tstreams.data.aerospike.{AerospikeStorageFactory, AerospikeStorageOptions}
import com.bwsw.tstreams.data.cassandra.{CassandraStorageFactory, CassandraStorageOptions}
import com.bwsw.tstreams.generator.IUUIDGenerator
import com.bwsw.tstreams.metadata.{MetadataStorage, MetadataStorageFactory}
import com.bwsw.tstreams.policy.AbstractPolicy
import com.bwsw.tstreams.streams.BasicStream
import com.bwsw.tstreams.velocity.RoundRobinPolicyCreator
import org.slf4j.LoggerFactory

import scala.collection.mutable.{HashMap, Map}

/**
  * Class which holds definitions for UniversalFactory
  */
object TSF_Dictionary {


  /**
    * UF_Dictionary metadata scope
    */
  object Metadata {

    /**
      * UF_Dictionary.Metadata cluster scope
      */
    object Cluster {
      /**
        * endpoint list of the metadata datastore, comma separated: host1:port1,host2:port2,host3:port3,...
        */
      val endpoints = "metadata.cluster.endpoints"

      /**
        * keyspace for metadata storage
        */
      val namespace = "metadata.cluster.namespace"
      /**
        * login of the user which can access to the metadata store
        */
      val login = "metadata.cluster.login"
      /**
        * password of the user which can access to the metadata store
        */
      val password = "metadata.cluster.password"
    }
  }

  /**
    * UF_Dictionary data scope
    */
  object Data {

    /**
      * UF_Dictionary cluster scope
      */
    object Cluster {
      /**
        *   the database driver which is used for data storage (currently UniversalFactory supports aerospike
        */
      val driver = "data.cluster.driver"
      /**
        * the name of akka actor system used with t-streams
        */
      val endpoints = "data.cluster.endpoints"
      /**
        * endpoints list of the database where txn data is stored, comma separated: host1:port1,host2:port2,host3:port3,...
        */
      val namespace = "data.cluster.namespace"
      /**
        * login name of the user which can access the data storage
        */
      val login = "data.cluster.login"
      /**
        * password of the user which can access the data storage
        */
      val password = "data.cluster.password"

      /**
        * Stores consts for UF_Dictionary.Data.Cluster
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
      }

      /**
        * scope UF_Dictionary.Data.Cluster aerospike engine
        */
      object Aerospike {
        /**
          * user can specify write policy if required
          */
        val write_policy = "data.cluster.aerospike.write-policy"
        /**
          * user can specify read policy if required
          */
        val read_policy = "data.cluster.aerospike.read-policy"
        /**
          * user can specify client policy
          */
        val client_policy = "data.cluster.aerospike.client-policy"

      }
    }
  }

  /**
    * UF_Dictionary coordination scope
    */
  object Coordination {
    /**
      * endpoint list for zookeeper coordination service, comma separated: host1:port1,host2:port2,host3:port3,...
      */
    val endpoints = "coordination.endpoints"
    /**
      * ZK root node which holds coordination tree
      */
    val root = "coordination.root"
    /**
      * ZK ttl for coordination
      */
    val ttl = "coordination.ttl"

    /**
      * ZK connection timeout
      */
    val connection_timeout = "coordination.connection-timeout"
  }

  /**
    * UF_Dictionary stream scope
    */
  object Stream {
    /**
      * name of the stream to work with
      */
    val name = "stream.name"
    /**
      * amount of stream partitions
      */
    val partitions = "stream.partitions"
    /**
      * stream time to leave (data expunged from datastore after that time)
      */
    val ttl = "stream.ttl"
    /**
      * random string description
      */
    val description = "stream.description"
  }

  /**
    * UF_Dictionary producer scope
    */
  object Producer {

    /**
      * amount of threads which handles works with transactions on master
      */
    val thread_pool = "producer.thread-pool"

    /**
      * hostname or ip of producer master listener
      */
    val master_bind_host = "producer.master-bind-host"
    /**
      * port of producer master listener
      */
    val master_bind_port = "producer.master-bind-port"
    /**
      * Transport timeout is maximum time to wait for master to respond
      */
    val master_timeout = "producer.master-timeout" //TODO: fix internals transport->master


    object Transaction {
      /**
        * TTL of transaction to wait until determine it's broken
        */
      val ttl = "producer.transaction.ttl"
      /**
        * Time to wait for successful end of opening operation on master for transaction
        */
      val open_maxwait = "producer.transaction.open-maxwait"
      /**
        * Time to update transaction state (keep it alive for long transactions)
        */
      val keep_alive = "producer.transaction.keep-alive"
      /**
        * amount of data items to batch when write data into txn
        */
      val data_write_batch_size = "producer.transaction.data-write-batch-size"
      /**
        * policy to distribute transactions over stream partitions
        */
      val distribution_policy = "producer.transaction.distribution-policy" // TODO: fix internals write->distribution

      /**
        * UF_Dictionary.Producer.Transaction consts scope
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
    * UF_Dictionary consumer scope
    */
  object Consumer {
    /**
      * name of consumer
      */
    val name = "producer.name"

    /**
      * amount of transactions to preload from C* to avoid additional select ops
      */
    val transaction_preload = "consumer.transaction-preload"
    /**
      * amount of data items to load at once from data storage
      */
    val data_preload = "consumer.data-preload"

    /**
      * UF_Dictionary.Consumer subscriber scope
      */
    object Subscriber {
      /**
        * host/ip to bind
        */
      val bind_host = "consumer.subscriber.bind-host"
      /**
        * port to bind
        */
      val bind_port = "consumer.subscriber.bind-port"

      /**
        * persistent queue path (fast disk where to store bursted data
        */
      val persistent_queue_path = "consumer.subscriber.persistent-queue.path"

      /**
        * thread pool size
        */
      val thread_pool = "consumer.subscriber.thread-pool"

    }
  }

}

/**
  * Created by ivan on 21.07.16.
  */
class TStreamsFactory(envname: String = "T-streams") {

  private val logger  = LoggerFactory.getLogger(this.getClass)
  private val lock    = new ReentrantLock(true)
  val propertyMap     = new HashMap[String,Any]()
  var isClosed        = false

  // common
  implicit val system = ActorSystem(envname.toString)

  /**
    * returnes actor system for whole world
    * @return
    */
  def getActorSystem() = system

  // metadata cluster scope
  propertyMap += (TSF_Dictionary.Metadata.Cluster.endpoints  -> "localhost:9042")
  propertyMap += (TSF_Dictionary.Metadata.Cluster.namespace   -> "test")
  propertyMap += (TSF_Dictionary.Metadata.Cluster.login      -> null)
  propertyMap += (TSF_Dictionary.Metadata.Cluster.password   -> null)

  // data cluster scope
  propertyMap += (TSF_Dictionary.Data.Cluster.driver         -> TSF_Dictionary.Data.Cluster.Consts.DATA_DRIVER_AEROSPIKE)
  propertyMap += (TSF_Dictionary.Data.Cluster.endpoints      -> "localhost:3000")
  propertyMap += (TSF_Dictionary.Data.Cluster.namespace      -> "test")
  propertyMap += (TSF_Dictionary.Data.Cluster.login          -> null)
  propertyMap += (TSF_Dictionary.Data.Cluster.password       -> null)
  propertyMap += (TSF_Dictionary.Data.Cluster.Aerospike.write_policy      -> null)
  propertyMap += (TSF_Dictionary.Data.Cluster.Aerospike.read_policy       -> null)
  propertyMap += (TSF_Dictionary.Data.Cluster.Aerospike.client_policy     -> null)


  // coordination scope
  propertyMap += (TSF_Dictionary.Coordination.endpoints          -> "localhost:2181")
  propertyMap += (TSF_Dictionary.Coordination.root               -> "/t-streams")

  val Coordination_ttl_default                                  = 7
  val Coordination_ttl_min                                      = 1
  val Coordination_ttl_max                                      = 30
  propertyMap += (TSF_Dictionary.Coordination.ttl                -> Coordination_ttl_default)

  val Coordination_connection_timeout_default                   = 7
  val Coordination_connection_timeout_min                       = 1
  val Coordination_connection_timeout_max                       = 30
  propertyMap += (TSF_Dictionary.Coordination.connection_timeout -> Coordination_connection_timeout_default)

  // stream scope
  propertyMap += (TSF_Dictionary.Stream.name                 -> "test")

  val Stream_partitions_default                             = 1
  val Stream_partitions_min                                 = 1
  val Stream_partitions_max                                 = 100000000
  propertyMap += (TSF_Dictionary.Stream.partitions           -> Stream_partitions_default)

  val Stream_ttl_default                                    = 60 * 60 * 24
  val Stream_ttl_min                                        = 60
  val Stream_ttl_max                                        = 315360000
  propertyMap += (TSF_Dictionary.Stream.ttl                  -> Stream_ttl_default)
  propertyMap += (TSF_Dictionary.Stream.description          -> "Test stream")

  // producer scope
  propertyMap += (TSF_Dictionary.Producer.master_bind_host                     -> "localhost")
  propertyMap += (TSF_Dictionary.Producer.master_bind_port                     -> 18000)
  val Producer_master_timeout_default                                         = 5
  val Producer_master_timeout_min                                             = 1
  val Producer_master_timeout_max                                             = 10
  propertyMap += (TSF_Dictionary.Producer.master_timeout                       -> Producer_master_timeout_default)

  val Producer_transaction_ttl_default                                        = 6
  val Producer_transaction_ttl_min                                            = 3
  val Producer_transaction_ttl_max                                            = 15
  propertyMap += (TSF_Dictionary.Producer.Transaction.ttl                      -> Producer_transaction_ttl_default)

  val Producer_transaction_open_maxwait_default                               = 5
  val Producer_transaction_open_maxwait_min                                   = 1
  val Producer_transaction_open_maxwait_max                                   = 10
  propertyMap += (TSF_Dictionary.Producer.Transaction.open_maxwait             -> Producer_transaction_open_maxwait_default)

  val Producer_transaction_keep_alive_default                                 = 1
  val Producer_transaction_keep_alive_min                                     = 1
  val Producer_transaction_keep_alive_max                                     = 2
  propertyMap += (TSF_Dictionary.Producer.Transaction.keep_alive               -> Producer_transaction_keep_alive_default)

  val Producer_transaction_data_write_batch_size_default                      = 100
  val Producer_transaction_data_write_batch_size_min                          = 1
  val Producer_transaction_data_write_batch_size_max                          = 1000
  propertyMap += (TSF_Dictionary.Producer.Transaction.data_write_batch_size    -> Producer_transaction_data_write_batch_size_default)
  propertyMap += (TSF_Dictionary.Producer.Transaction.distribution_policy      -> TSF_Dictionary.Producer.Transaction.Consts.DISTRIBUTION_POLICY_RR)

  val Producer_thread_pool_default                                            = 4
  val Producer_thread_pool_min                                                = 1
  val Producer_thread_pool_max                                                = 64
  propertyMap += (TSF_Dictionary.Producer.thread_pool                          -> Producer_thread_pool_default)

  // consumer scope
  val Consumer_transaction_preload_default                                = 10
  val Consumer_transaction_preload_min                                    = 1
  val Consumer_transaction_preload_max                                    = 100
  propertyMap += (TSF_Dictionary.Consumer.transaction_preload              -> Consumer_transaction_preload_default)
  val Consumer_data_preload_default                                       = 100
  val Consumer_data_preload_min                                           = 10
  val Consumer_data_preload_max                                           = 200
  propertyMap += (TSF_Dictionary.Consumer.data_preload                     -> Consumer_data_preload_default)
  propertyMap += (TSF_Dictionary.Consumer.Subscriber.bind_host             -> "localhost")
  propertyMap += (TSF_Dictionary.Consumer.Subscriber.bind_port             -> 18001)
  propertyMap += (TSF_Dictionary.Consumer.Subscriber.persistent_queue_path -> "/tmp")

  val Subscriber_thread_pool_default                                            = 4
  val Subscriber_thread_pool_min                                                = 1
  val Subscriber_thread_pool_max                                                = 64
  propertyMap += (TSF_Dictionary.Consumer.Subscriber.thread_pool                -> Subscriber_thread_pool_default)


  //metadata/data factories
  val msFactory               = new MetadataStorageFactory
  val aerospikeStorageFactory = new AerospikeStorageFactory
  val cassandraStorageFactory = new CassandraStorageFactory

  /**
    *
    * @param key
    * @param value
    * @return
    */
  def setProperty(key: String, value: Any): TStreamsFactory = {
    logger.info("set property " + key + " = " + value)
    if(propertyMap contains key)
      propertyMap += (key -> value)
    else
      throw new IllegalArgumentException("Property " + key + " is unknown and can not be altered.")
    return this
  }

  /**
    *
    * @param key
    * @return
    */
  def getProperty(key: String): Any = {
    val v = propertyMap get key
    logger.info("get property " + key + " = " + v.getOrElse(null))
    v.getOrElse(null)
  }

  /** variant method to get option as int with default value if null
    * @param key key to request
    * @param default assign it if the value received from options is null
    * @return
    */
  private def pAsInt(key: String, default: Int = 0): Int = if(null == getProperty(key)) default else Integer.parseInt(getProperty(key).toString)

  /**
    * variant method to get option as string with default value if null
    * @param key key to request
    * @param default assign it if the value received from options is null
    * @return
    */
  private def pAsString(key: String, default: String = null): String = {
    val s = getProperty(key)
    if(null == s)
      return default
    s.toString
  }

  /**
    * checks that int inside interval
    * @param value
    * @param min
    * @param max
    * @return
    */
  private def pAssertIntRange(value: Int, min: Int, max: Int): Int =  {
    assert(value >= min && value <= max)
    value
  }

  /**
    * transforms host:port,host:port to list(Host, Host) for Aerospike
    * @param h
    * @return
    */
  private def getAerospikeCompatibleHostList(h: String): List[Host] =
  h.split(',').map((sh: String) => new Host(sh.split(':').head, Integer.parseInt(sh.split(':').tail.head))).toList

  /**
    * transforms host:port,host:port to list(InetSocketAddr, InetSocketAddr) for C*
    * @param h
    * @return
    */
  private def getInetSocketAddressCompatibleHostList(h: String): List[InetSocketAddress] =
  h.split(',').map((sh: String) => new InetSocketAddress(sh.split(':').head, Integer.parseInt(sh.split(':').tail.head))).toList



  /**
    * common routine allows getting ready to use data store object
    *
    * @return
    */
  private def getDataStorage(): IStorage[Array[Byte]] = {
    if (pAsString(TSF_Dictionary.Data.Cluster.driver) == TSF_Dictionary.Data.Cluster.Consts.DATA_DRIVER_AEROSPIKE)
    {
      val dsf = aerospikeStorageFactory

      // construct client policy
      var cp: ClientPolicy = null
      if (getProperty(TSF_Dictionary.Data.Cluster.Aerospike.client_policy) == null)
        cp = new ClientPolicy()
      else
        cp = getProperty(TSF_Dictionary.Data.Cluster.Aerospike.client_policy).asInstanceOf[ClientPolicy]

      cp.user     = pAsString(TSF_Dictionary.Data.Cluster.login, null)
      cp.password = pAsString(TSF_Dictionary.Data.Cluster.password, null)

      // construct write policy
      var wp: WritePolicy = null
      if (getProperty(TSF_Dictionary.Data.Cluster.Aerospike.write_policy) == null)
        wp = new WritePolicy()
      else
        wp = getProperty(TSF_Dictionary.Data.Cluster.Aerospike.write_policy).asInstanceOf[WritePolicy]

      // construct write policy
      var rp: Policy = null
      if (getProperty(TSF_Dictionary.Data.Cluster.Aerospike.read_policy) == null)
        rp = new WritePolicy()
      else
        rp = getProperty(TSF_Dictionary.Data.Cluster.Aerospike.read_policy).asInstanceOf[Policy]

      val namespace = pAsString(TSF_Dictionary.Data.Cluster.namespace)
      val data_cluster_endpoints = pAsString(TSF_Dictionary.Data.Cluster.endpoints)
      assert(namespace != null)
      assert(data_cluster_endpoints != null)

      val opts = new AerospikeStorageOptions(
        namespace     = namespace,
        hosts         = getAerospikeCompatibleHostList(data_cluster_endpoints),
        clientPolicy  = cp,
        writePolicy   = wp,
        readPolicy    = rp)

      // create instance of aerospike data storage
      return dsf.getInstance(opts)

    }
    else if (pAsString(TSF_Dictionary.Data.Cluster.driver) == TSF_Dictionary.Data.Cluster.Consts.DATA_DRIVER_CASSANDRA)
    {
      val dsf = cassandraStorageFactory

      val login     = pAsString(TSF_Dictionary.Data.Cluster.login, null)
      val password  = pAsString(TSF_Dictionary.Data.Cluster.password, null)

      assert(pAsString(TSF_Dictionary.Data.Cluster.namespace) != null)
      assert(pAsString(TSF_Dictionary.Data.Cluster.endpoints) != null)

      val opts = new CassandraStorageOptions(
        keyspace        = pAsString(TSF_Dictionary.Data.Cluster.namespace),
        cassandraHosts  = getInetSocketAddressCompatibleHostList(pAsString(TSF_Dictionary.Data.Cluster.endpoints)),
        login           = login,
        password        = password
      )

      return dsf.getInstance(opts)
    }
    else
    {
      throw new InvalidParameterException("Only UF_Dictionary.Data.Cluster.Consts.DATA_DRIVER_CASSANDRA and " +
        "UF_Dictionary.Data.Cluster.Consts.DATA_DRIVER_AEROSPIKE engines " +
        "are supported currently in UniversalFactory. Received: " + pAsString(TSF_Dictionary.Data.Cluster.driver))
    }
    null
  }

  /**
    * common routine which allows getting ready to use metadata object
    * @return
    */
  private def getMetadataStorage(): MetadataStorage = {
    val login     = pAsString(TSF_Dictionary.Metadata.Cluster.login, null)
    val password  = pAsString(TSF_Dictionary.Metadata.Cluster.password, null)

    assert(pAsString(TSF_Dictionary.Metadata.Cluster.namespace) != null)
    assert(pAsString(TSF_Dictionary.Metadata.Cluster.endpoints) != null)

    // construct metadata storage
    return msFactory.getInstance(
      keyspace        = pAsString(TSF_Dictionary.Metadata.Cluster.namespace),
      cassandraHosts  = getInetSocketAddressCompatibleHostList(pAsString(TSF_Dictionary.Metadata.Cluster.endpoints)),
      login           = login,
      password        = password)
  }

  /**
    * common routine which allows to get ready to use stream object by env
    * @param metadatastorage
    * @param datastorage
    * @return
    */
  private def getStream(metadatastorage: MetadataStorage, datastorage: IStorage[Array[Byte]]): BasicStream[Array[Byte]] = {
    assert(pAsString(TSF_Dictionary.Stream.name) != null)
    pAssertIntRange(pAsInt(TSF_Dictionary.Stream.partitions, Stream_partitions_default), Stream_partitions_min, Stream_partitions_max)
    pAssertIntRange(pAsInt(TSF_Dictionary.Stream.ttl, Stream_ttl_default), Stream_ttl_min, Stream_ttl_max)

    // construct stream
    val stream = new BasicStream[Array[Byte]](
      name            = pAsString(TSF_Dictionary.Stream.name),
      partitions      = pAsInt(TSF_Dictionary.Stream.partitions, Stream_partitions_default),
      metadataStorage = metadatastorage,
      dataStorage     = datastorage,
      ttl             = pAsInt(TSF_Dictionary.Stream.ttl, Stream_ttl_default),
      description     = pAsString(TSF_Dictionary.Stream.description, ""))
    return stream
  }

  /**
    * reusable method which returns consumer options object
    */
  private def getBasicConsumerOptions[USERTYPE](stream          : BasicStream[Array[Byte]],
                                                partitions      : List[Int],
                                                converter       : IConverter[Array[Byte],USERTYPE],
                                                txnGenerator    : IUUIDGenerator,
                                                offset          : IOffset,
                                                isUseLastOffset : Boolean = true): BasicConsumerOptions[Array[Byte],USERTYPE] = {
    val consumer_transaction_preload = pAsInt(TSF_Dictionary.Consumer.transaction_preload, Consumer_transaction_preload_default)
    pAssertIntRange(consumer_transaction_preload, Consumer_transaction_preload_min, Consumer_transaction_preload_max)

    val consumer_data_preload = pAsInt(TSF_Dictionary.Consumer.data_preload, Consumer_data_preload_default)
    pAssertIntRange(consumer_data_preload, Consumer_data_preload_min, Consumer_data_preload_max)

    val consumerOptions = new BasicConsumerOptions[Array[Byte], USERTYPE](
      transactionsPreload       = consumer_transaction_preload,
      dataPreload               = consumer_data_preload,
      consumerKeepAliveInterval = 5,  // TODO: deprecated, unused, to remove
      converter                 = converter,
      readPolicy                = RoundRobinPolicyCreator.getRoundRobinPolicy(stream, partitions),
      offset                    = offset,
      txnGenerator              = txnGenerator,
      useLastOffset             = isUseLastOffset)

    consumerOptions
  }

  /**
    * returns ready to use producer object
    * @param name Producer name
    * @param isLowPriority
    * @param txnGenerator
    * @param converter
    * @param partitions
    * @tparam USERTYPE - type convert data from
    * @return
    */
  def getProducer[USERTYPE](name: String,
                            isLowPriority : Boolean = false,
                            txnGenerator  : IUUIDGenerator,
                            converter     : IConverter[USERTYPE,Array[Byte]],
                            partitions    : List[Int]
                           ): BasicProducer[USERTYPE,Array[Byte]] = {

    lock.lock()

    if(isClosed)
      throw new IllegalStateException("TStreamsFactory is closed. This is the illegal usage of the object.")

    val ds: IStorage[Array[Byte]]         = getDataStorage()
    val ms: MetadataStorage               = getMetadataStorage()
    val stream: BasicStream[Array[Byte]]  = getStream(metadatastorage = ms, datastorage = ds)

    assert(pAsString(TSF_Dictionary.Producer.master_bind_host) != null)
    assert(pAsString(TSF_Dictionary.Producer.master_bind_port) != null)
    assert(pAsString(TSF_Dictionary.Coordination.endpoints)    != null)
    assert(pAsString(TSF_Dictionary.Coordination.root)    != null)

    pAssertIntRange(pAsInt(TSF_Dictionary.Coordination.ttl, Coordination_ttl_default), Coordination_ttl_min, Coordination_ttl_max)
    pAssertIntRange(pAsInt(TSF_Dictionary.Producer.master_timeout, Producer_master_timeout_default), Producer_master_timeout_min, Producer_master_timeout_max)
    pAssertIntRange(pAsInt(TSF_Dictionary.Coordination.connection_timeout, Coordination_connection_timeout_default),
      Coordination_connection_timeout_min, Coordination_connection_timeout_max)

    // construct coordination agent options
    val cao = new ProducerCoordinationOptions(
      agentAddress            = pAsString(TSF_Dictionary.Producer.master_bind_host) + ":" + pAsString(TSF_Dictionary.Producer.master_bind_port),
      zkHosts                 = getInetSocketAddressCompatibleHostList(pAsString(TSF_Dictionary.Coordination.endpoints)),
      zkRootPath              = pAsString(TSF_Dictionary.Coordination.root),
      zkSessionTimeout        = pAsInt(TSF_Dictionary.Coordination.ttl, Coordination_ttl_default),
      isLowPriorityToBeMaster = isLowPriority,
      transport               = new TcpTransport,
      transportTimeout        = pAsInt(TSF_Dictionary.Producer.master_timeout, Producer_master_timeout_default),
      zkConnectionTimeout     = pAsInt(TSF_Dictionary.Coordination.connection_timeout, Coordination_connection_timeout_default))


    var writePolicy: AbstractPolicy = null

    if (pAsString(TSF_Dictionary.Producer.Transaction.distribution_policy) ==
      TSF_Dictionary.Producer.Transaction.Consts.DISTRIBUTION_POLICY_RR) {
      writePolicy = RoundRobinPolicyCreator.getRoundRobinPolicy(stream, partitions)
    }
    else
    {
      throw new InvalidParameterException("Only UF_Dictionary.Producer.Transaction.Consts.DISTRIBUTION_POLICY_RR policy " +
        "is supported currently in UniversalFactory.")
    }

    pAssertIntRange(pAsInt(TSF_Dictionary.Producer.Transaction.ttl, Producer_transaction_ttl_default), Producer_transaction_ttl_min, Producer_transaction_ttl_max)
    pAssertIntRange(pAsInt(TSF_Dictionary.Producer.Transaction.keep_alive, Producer_transaction_keep_alive_default), Producer_transaction_keep_alive_min, Producer_transaction_keep_alive_max)
    assert(pAsInt(TSF_Dictionary.Producer.Transaction.ttl, Producer_transaction_ttl_default) >=
      pAsInt(TSF_Dictionary.Producer.Transaction.keep_alive, Producer_transaction_keep_alive_default) * 3)

    var insertType: InsertType = SingleElementInsert

    val insertCnt = pAsInt(TSF_Dictionary.Producer.Transaction.data_write_batch_size, Producer_transaction_data_write_batch_size_default)
    pAssertIntRange(insertCnt,
      Producer_transaction_data_write_batch_size_min, Producer_transaction_data_write_batch_size_max)

    if (insertCnt > 1)
      insertType = BatchInsert(insertCnt)

    val po = new BasicProducerOptions[USERTYPE, Array[Byte]](
      transactionTTL                = pAsInt(TSF_Dictionary.Producer.Transaction.ttl, Producer_transaction_ttl_default),
      transactionKeepAliveInterval  = pAsInt(TSF_Dictionary.Producer.Transaction.keep_alive, Producer_transaction_keep_alive_default),
      producerKeepAliveInterval     = 1, // TODO: deprecated, remove when https://github.com/bwsw/t-streams/issues/19 will be fixed!
      writePolicy                   = writePolicy,
      insertType                    = SingleElementInsert,
      txnGenerator                  = txnGenerator,
      producerCoordinationSettings  = cao,
      converter                     = converter)

    // TODO FIX: Stream is not required as argument for BasicProducer - it's already in options
    val producer = new BasicProducer[USERTYPE, Array[Byte]](
      name              = name,
      stream            = stream,
      producerOptions   = po)

    lock.unlock()
    producer
  }

  /**
    * returns ready to use consumer object
    * @param name Consumer name
    * @param txnGenerator
    * @param converter
    * @param partitions
    * @tparam USERTYPE type to convert data to
    * @return
    */
  def getConsumer[USERTYPE](name          : String,
                            txnGenerator  : IUUIDGenerator,
                            converter     : IConverter[Array[Byte],USERTYPE],
                            partitions    : List[Int],
                            offset        : IOffset,
                            isUseLastOffset : Boolean = true
                           ): BasicConsumer[Array[Byte],USERTYPE] = {

    lock.lock()

    if(isClosed)
      throw new IllegalStateException("TStreamsFactory is closed. This is the illegal usage of the object.")

    val ds: IStorage[Array[Byte]]         = getDataStorage()
    val ms: MetadataStorage               = getMetadataStorage()
    val stream: BasicStream[Array[Byte]]  = getStream(metadatastorage = ms, datastorage = ds)
    val consumerOptions = getBasicConsumerOptions(txnGenerator  = txnGenerator,
                                                  stream        = stream,
                                                  partitions    = partitions,
                                                  converter     = converter,
                                                  offset        = offset,
                                                  isUseLastOffset = isUseLastOffset)

    val consumer = new BasicConsumer(name, stream, consumerOptions)  // TODO FIX: Stream is not required as argument for BasicConsumer - it's already in options
    lock.unlock()

    consumer
  }

  /**
    * returns ready to use subscribing consumer object
    * @param txnGenerator
    * @param converter
    * @param partitions
    * @param callback
    * @tparam USERTYPE - type to convert data to
    * @return
    */
  def getSubscriber[USERTYPE](name          : String,
                              txnGenerator  : IUUIDGenerator,
                              converter     : IConverter[Array[Byte],USERTYPE],
                              partitions    : List[Int],
                              offset        : IOffset,
                              isUseLastOffset : Boolean = true,
                              callback: BasicSubscriberCallback[Array[Byte],USERTYPE]): BasicSubscribingConsumer[Array[Byte],USERTYPE] = {
    lock.lock()
    if(isClosed)
      throw new IllegalStateException("TStreamsFactory is closed. This is the illegal usage of the object.")

    val ds: IStorage[Array[Byte]]         = getDataStorage()
    val ms: MetadataStorage               = getMetadataStorage()
    val stream: BasicStream[Array[Byte]]  = getStream(metadatastorage = ms, datastorage = ds)

    val consumerOptions = getBasicConsumerOptions(txnGenerator  = txnGenerator,
                                                  stream        = stream,
                                                  partitions    = partitions,
                                                  converter     = converter,
                                                  offset        = offset,
                                                  isUseLastOffset = isUseLastOffset)

    //val consumer = new BasicConsumer(name, stream, consumerOptions)
    val bind_host = pAsString(TSF_Dictionary.Consumer.Subscriber.bind_host)
    assert(bind_host != null)
    val bind_port = pAsString(TSF_Dictionary.Consumer.Subscriber.bind_port)
    assert(bind_port != null)
    val endpoints = pAsString(TSF_Dictionary.Coordination.endpoints)
    assert(endpoints != null)
    val root = pAsString(TSF_Dictionary.Coordination.root)
    assert(root != null)

    val ttl = pAsInt(TSF_Dictionary.Coordination.ttl, Coordination_ttl_default)
    pAssertIntRange(ttl, Coordination_ttl_min, Coordination_ttl_max)
    val conn_timeout = pAsInt(TSF_Dictionary.Coordination.connection_timeout, Coordination_connection_timeout_default)
    pAssertIntRange(conn_timeout,
      Coordination_connection_timeout_min, Coordination_connection_timeout_max)

    val thread_pool = pAsInt(TSF_Dictionary.Consumer.Subscriber.thread_pool, Subscriber_thread_pool_default)
    pAssertIntRange(thread_pool,
      Subscriber_thread_pool_min, Subscriber_thread_pool_max)

    val coordinationOptions = new SubscriberCoordinationOptions(
                                                agentAddress  = bind_host + ":" + bind_port,
                                                zkRootPath    = root,
                                                zkHosts       = getInetSocketAddressCompatibleHostList(endpoints),
                                                zkSessionTimeout    = ttl,
                                                zkConnectionTimeout = conn_timeout,
                                                threadPoolAmount    = thread_pool)

    val queue_path = pAsString(TSF_Dictionary.Consumer.Subscriber.persistent_queue_path)
    assert(queue_path != null)

    val subscribeConsumer = new BasicSubscribingConsumer[Array[Byte],USERTYPE](
                      name                          = name,
                      stream                        = stream, // TODO FIX: Stream is not required as argument for BasicSubscribingConsumer - it's already in options
                      options                       = consumerOptions,
                      subscriberCoordinationOptions = coordinationOptions,
                      callBack                      = callback,
                      persistentQueuePath           = queue_path)

    subscribeConsumer.start()

    lock.unlock()

    subscribeConsumer
  }

  def close(): Unit = {
    lock.lock()

    if(isClosed)
      throw new IllegalStateException("TStreamsFactory is closed. This is repeatable close operation.")

    isClosed = true
    msFactory.closeFactory()
    cassandraStorageFactory.closeFactory()
    aerospikeStorageFactory.closeFactory()

    lock.unlock()
  }

}
