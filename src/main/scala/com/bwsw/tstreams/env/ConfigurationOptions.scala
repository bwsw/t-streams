package com.bwsw.tstreams.env

/**
  * Class which holds definitions for UniversalFactory
  */
object ConfigurationOptions {

  object Common {
    val authenticationKey = "common.authentication-key"
  }

  /**
    * TSF_Dictionary storage scope
    */
  object StorageClient {

    object Zookeeper {
      val prefix = "storage-client.zk.prefix"
    }

    val connectionTimeoutMs = "storage-client.connection-timeout-ms"
    val requestTimeoutMs = "storage-client.request-timeout-ms"
    val requestTimeoutRetryCount = "storage-client.request-timeout-retry-count"
    val retryDelayMs = "storage-client.retry-delay-ms"
    val threadPool = "storage-client.thread-pool"
  }

  object Stream {
    val name = "stream.name"
    val description = "stream.description"
    val partitionsCount = "stream.partitions-count"
    /**
      * stream time to leave (data expunged from data store after that time)
      */
    val ttlSec = "stream.ttl"
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
    val sessionTimeoutMs = "coordination.ttl-ms"

    /**
      * ZK connection timeout
      */
    val connectionTimeoutMs = "coordination.connection-timeout"

    /**
      *
      */
    val retryDelayMs = "coordination.zk.retry-delay-ms"

    /**
      *
      */
    val retryCount = "coordination.zk.retry-count"
  }


  /**
    * TSF_Dictionary producer scope
    */
  object Producer {

    /**
      * amount of threads which handles works with transactions on master
      */
    val threadPoolSize = "producer.thread-pool"

    /**
      * amount of publisher threads in a thread pool (default 1)
      */
    val notifyJobsThreadPoolSize = "producer.thread-pool.notify-jobs-thread-pool-size"

    /**
      * hostname or ip of producer master listener
      */
    val bindHost = "producer.bind-host"
    /**
      * port of producer master listener
      */
    val bindPort = "producer.bind-port"
    /**
      * Transport timeout is maximum time to wait for master to respond
      */
    val openTimeoutMs = "producer.open-timeout-ms"


    object Transaction {
      /**
        * TTL of transaction to wait until determine it's broken
        */
      val ttlMs = "producer.transaction.ttl"

      /**
        * Time to update transaction state (keep it alive for long transactions)
        */
      val keepAliveMs = "producer.transaction.keep-alive"
      /**
        * amount of data items to batch when write data into transaction
        */
      val batchSize = "producer.transaction.data-write-batch-size"
      /**
        * policy to distribute transactions over stream partitions
        */
      val distributionPolicy = "producer.transaction.distribution-policy"

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
    val transactionPreload = "consumer.transaction-preload"
    /**
      * amount of data items to load at once from data storage
      */
    val dataPreload = "consumer.data-preload"

    /**
      * TSF_Dictionary.Consumer subscriber scope
      */
    object Subscriber {
      /**
        * host/ip to bind
        */
      val bindHost = "consumer.subscriber.bind-host"
      /**
        * port to bind
        */
      val bindPort = "consumer.subscriber.bind-port"

      /**
        * persistent queue path (fast disk where to store burst data
        */
      val persistentQueuePath = "consumer.subscriber.persistent-queue.path"

      /**
        * thread pool size
        */
      val transactionBufferThreadPoolSize = "consumer.subscriber.transaction-buffer-thread-pool"

      /**
        * processing engines pool
        */
      val processingEnginesThreadPoolSize = "consumer.subscriber.processing-engines-thread-pool"

      /**
        * thread pool size
        */
      val pollingFrequencyDelayMs = "consumer.subscriber.polling-frequency-delay"

      /**
        * maximum amount of transactions in-flight in a map
        */
      val transactionQueueMaxLengthThreshold = "consumer.subscriber.transaction-queue-max-length-threshold"

    }

  }

}