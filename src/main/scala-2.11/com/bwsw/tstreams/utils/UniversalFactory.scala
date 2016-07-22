package com.bwsw.tstreams.utils

import java.net.{InetSocketAddress}
import java.security.InvalidParameterException

import com.aerospike.client.Host
import com.aerospike.client.policy.{Policy, WritePolicy, ClientPolicy}
import com.bwsw.tstreams.agents.consumer.BasicConsumer
import com.bwsw.tstreams.agents.consumer.subscriber.{BasicSubscriberCallback, BasicSubscribingConsumer}
import com.bwsw.tstreams.agents.producer.BasicProducer
import com.bwsw.tstreams.converter.IConverter
import com.bwsw.tstreams.data.IStorage
import com.bwsw.tstreams.data.aerospike.{AerospikeStorageOptions, AerospikeStorageFactory}
import com.bwsw.tstreams.data.cassandra.{CassandraStorageOptions, CassandraStorageFactory}
import com.bwsw.tstreams.generator.IUUIDGenerator
import com.bwsw.tstreams.metadata.MetadataStorageFactory
import com.datastax.driver.core.policies.RoundRobinPolicy
import org.omg.PortableServer.THREAD_POLICY_ID

import scala.collection.mutable.{Map, HashMap}

/**
  * Class which holds definitions for UniversalFactory
  */
object UF_Dictionary {

  /**
    * UF_Dictionary actor-system scope
    */
  object ActorSystem {
    /**
      * name of the actor system which is used with t-streams
      */
    val name = "actor-system.name"
  }

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
      val persistent_queue_path = "subscriber.persistent-queue.path"
    }
  }

}

/**
  * Created by ivan on 21.07.16.
  */
class UniversalFactory {

  var propertyMap = new HashMap[String,Any]()

  // common
  propertyMap += (UF_Dictionary.ActorSystem.name            -> "T-Streams")

  // metadata cluster scope
  propertyMap += (UF_Dictionary.Metadata.Cluster.endpoints  -> "localhost:9042")
  propertyMap += (UF_Dictionary.Metadata.Cluster.login      -> null)
  propertyMap += (UF_Dictionary.Metadata.Cluster.password   -> null)

  // data cluster scope
  propertyMap += (UF_Dictionary.Data.Cluster.driver         -> UF_Dictionary.Data.Cluster.Consts.DATA_DRIVER_AEROSPIKE)
  propertyMap += (UF_Dictionary.Data.Cluster.endpoints      -> "localhost:3000")
  propertyMap += (UF_Dictionary.Data.Cluster.namespace      -> "test")
  propertyMap += (UF_Dictionary.Data.Cluster.login          -> null)
  propertyMap += (UF_Dictionary.Data.Cluster.password       -> null)
  propertyMap += (UF_Dictionary.Data.Cluster.Aerospike.write_policy      -> null)
  propertyMap += (UF_Dictionary.Data.Cluster.Aerospike.read_policy       -> null)
  propertyMap += (UF_Dictionary.Data.Cluster.Aerospike.client_policy     -> null)


  // coordination scope
  propertyMap += (UF_Dictionary.Coordination.endpoints          -> "localhost:2181")
  propertyMap += (UF_Dictionary.Coordination.root               -> "/t-streams")
  propertyMap += (UF_Dictionary.Coordination.ttl                -> 7)
  propertyMap += (UF_Dictionary.Coordination.connection_timeout -> 7)

  // stream scope
  propertyMap += (UF_Dictionary.Stream.name                 -> "test")
  propertyMap += (UF_Dictionary.Stream.partitions           -> 1)
  propertyMap += (UF_Dictionary.Stream.ttl                  -> 60 * 60 * 24)
  propertyMap += (UF_Dictionary.Stream.description          -> "Test stream")

  // producer scope
  propertyMap += (UF_Dictionary.Producer.master_bind_host                     -> "localhost")
  propertyMap += (UF_Dictionary.Producer.master_bind_port                     -> 18000)
  propertyMap += (UF_Dictionary.Producer.master_timeout                       -> 5)
  propertyMap += (UF_Dictionary.Producer.Transaction.ttl                      -> 6)
  propertyMap += (UF_Dictionary.Producer.Transaction.open_maxwait             -> 5)
  propertyMap += (UF_Dictionary.Producer.Transaction.keep_alive               -> 2)
  propertyMap += (UF_Dictionary.Producer.Transaction.data_write_batch_size    -> 100)
  propertyMap += (UF_Dictionary.Producer.Transaction.distribution_policy      -> UF_Dictionary.Producer.Transaction.Consts.DISTRIBUTION_POLICY_RR)
  propertyMap += (UF_Dictionary.Producer.thread_pool                          -> 4)

  // consumer scope
  propertyMap += (UF_Dictionary.Consumer.transaction_preload              -> 10)
  propertyMap += (UF_Dictionary.Consumer.data_preload                     -> 100)
  propertyMap += (UF_Dictionary.Consumer.Subscriber.bind_host             -> "localhost")
  propertyMap += (UF_Dictionary.Consumer.Subscriber.bind_port             -> 18001)
  propertyMap += (UF_Dictionary.Consumer.Subscriber.persistent_queue_path -> "/tmp")

  //metadata/data factories
  val msFactory = new MetadataStorageFactory

  /**
    *
    * @param key
    * @param value
    * @return
    */
  def setProperty(key: String, value: Any): UniversalFactory = {
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
  def getProperty(key: String): Any = propertyMap get key

  def getAerospikeCompatibleHostList(h: String): List[Host] =
    h.split(',').map((sh: String) => new Host(sh.split(':').head, Integer.parseInt(sh.split(':').tail.head))).toList

  def getCassandraCompatibleHostList(h: String): List[InetSocketAddress] =
    h.split(',').map((sh: String) => new InetSocketAddress(sh.split(':').head, Integer.parseInt(sh.split(':').tail.head))).toList

  /**
    *
    * @param isLowPriority
    * @param txnGenerator
    * @param converter
    * @param partitions
    * @tparam USERTYPE - type convert data from
    * @return
    */
  def getProducer[USERTYPE](isLowPriority : Boolean,
                       txnGenerator  : IUUIDGenerator,
                       converter     : IConverter[USERTYPE,Array[Byte]],
                       partitions    : List[Int]
                      ): BasicProducer[USERTYPE,Array[Byte]] = {

    var ds: IStorage[Array[Byte]] = null

    if ((propertyMap get UF_Dictionary.Data.Cluster.driver).equals(UF_Dictionary.Data.Cluster.Consts.DATA_DRIVER_AEROSPIKE))
    {
      val dsf = new AerospikeStorageFactory

      // construct client policy
      var cp: ClientPolicy = null
      if (propertyMap.get(UF_Dictionary.Data.Cluster.Aerospike.client_policy) == null)
        cp = new ClientPolicy()
      else
        cp = propertyMap.get(UF_Dictionary.Data.Cluster.Aerospike.client_policy).asInstanceOf[ClientPolicy]

      val dsLogin = propertyMap.get(UF_Dictionary.Data.Cluster.login)
      if (dsLogin != null)
        cp.user = dsLogin.toString

      val dsPassword = propertyMap.get(UF_Dictionary.Data.Cluster.password)
      if (dsPassword != null)
        cp.password = dsPassword.toString

      // construct write policy
      var wp: WritePolicy = null
      if (propertyMap.get(UF_Dictionary.Data.Cluster.Aerospike.write_policy) == null)
        wp = new WritePolicy()
      else
        wp = propertyMap.get(UF_Dictionary.Data.Cluster.Aerospike.write_policy).asInstanceOf[WritePolicy]

      // construct write policy
      var rp: Policy = null
      if (propertyMap.get(UF_Dictionary.Data.Cluster.Aerospike.read_policy) == null)
        rp = new WritePolicy()
      else
        rp = propertyMap.get(UF_Dictionary.Data.Cluster.Aerospike.read_policy).asInstanceOf[Policy]

      val opts = new AerospikeStorageOptions(
        namespace     = propertyMap.get(UF_Dictionary.Data.Cluster.namespace).toString,
        hosts         = getAerospikeCompatibleHostList(propertyMap.get(UF_Dictionary.Data.Cluster.endpoints).toString),
        clientPolicy  = cp,
        writePolicy   = wp,
        readPolicy    = rp)

      // create instance of aerospike data storage
      ds = dsf.getInstance(opts)

    }
    else if ((propertyMap get UF_Dictionary.Data.Cluster.driver).equals(UF_Dictionary.Data.Cluster.Consts.DATA_DRIVER_CASSANDRA))
    {
      val dsf = new CassandraStorageFactory

      val dsLogin = propertyMap.get(UF_Dictionary.Data.Cluster.login)
      val login = if (dsLogin != null) dsLogin.toString else null

      val dsPassword = propertyMap.get(UF_Dictionary.Data.Cluster.password)
      val password = if (dsPassword != null) dsPassword.toString else null

      val opts = new CassandraStorageOptions(
        keyspace = propertyMap.get(UF_Dictionary.Data.Cluster.namespace).toString,
        cassandraHosts = getCassandraCompatibleHostList(propertyMap.get(UF_Dictionary.Data.Cluster.endpoints).toString),
        login = login,
        password = password
      )

      ds = dsf.getInstance(opts)
    } else
    {
      throw new InvalidParameterException("Only UF_Dictionary.Data.Cluster.Consts.DATA_DRIVER_CASSANDRA and " +
                                          "UF_Dictionary.Data.Cluster.Consts.DATA_DRIVER_AEROSPIKE engines " +
                                          "are supported currently in UniversalFactory.")
    }

    null
  }

  /**
    *
    * @param txnGenerator
    * @param converter
    * @param partitions
    * @tparam USERTYPE type to convert data to
    * @return
    */
  def getConsumer[USERTYPE](txnGenerator : IUUIDGenerator,
                       converter : IConverter[Array[Byte],USERTYPE],
                       partitions : List[Int]
                      ): BasicConsumer[Array[Byte],USERTYPE] = {
    null
  }

  /**
    *
    * @param txnGenerator
    * @param converter
    * @param partitions
    * @param callback
    * @tparam USERTYPE - type to convert data to
    * @return
    */
  def getSubscriber[USERTYPE](txnGenerator : IUUIDGenerator,
                         converter : IConverter[Array[Byte],USERTYPE],
                         partitions : List[Int],
                         callback: BasicSubscriberCallback[Array[Byte],USERTYPE]): BasicSubscribingConsumer[Array[Byte],USERTYPE] = {
    null
  }

}
