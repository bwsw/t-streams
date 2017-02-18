package com.bwsw.tstreams.env

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

  object Stream {
    val name = "stream.name"
    val description = "stream.description"
    val partitionsCount = "stream.partitions-count"
    /**
      * stream time to leave (data expunged from datastore after that time)
      */
    val ttl = "stream.ttl"
  }

  /**
    * TSF_Dictionary coordination scope
    */
  object Coordination {
    /**
      * endpoint list for zookeeper coordination service, comma separated: host1:port1,host2:port2,host3:port3,...
      */
    val endpoints = "coordination.endpoints"
    /**
      * ZK root node which holds coordination tree
      */
    val prefix = "coordination.root"
    /**
      * ZK ttl for coordination
      */
    val sessionTimeoutMs = "coordination.ttl"

    /**
      * ZK connection timeout
      */
    val connectionTimeoutMs = "coordination.connection-timeout"

    /**
      * partition redistribution delay
      */
    val partitionsRedistributionDelaySec = "coordination.partition-redistribution-delay"
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