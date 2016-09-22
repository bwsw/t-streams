package com.bwsw.tstreams.common

import java.net.InetSocketAddress

import com.datastax.driver.core.ConsistencyLevel

/**
  * This code is from https://github.com/datastax/spark-cassandra-connector/
  * Stores configuration of a connection to Cassandra.
  * Provides information about cluster nodes, ports and optional credentials for authentication. */
case class CassandraConnectorConf(
                                   hosts: Set[InetSocketAddress],
                                   localDC: Option[String] = None,
                                   keepAliveMillis: Int = 5000,
                                   minReconnectionDelayMillis: Int = 1000,
                                   maxReconnectionDelayMillis: Int = 60000,
                                   queryRetryCount: Int = 10,
                                   connectTimeoutMillis: Int = 5000,
                                   readTimeoutMillis: Int = 120000,
                                   login: String = null,
                                   password: String = null,
                                   localCoreConnectionsPerHost: Int = 4,
                                   localMaxConnectionsPerHost: Int = 32,
                                   remoteCoreConnectionsPerHost: Int = 2,
                                   remoteMaxConnectionsPerHost: Int = 8,
                                   localMaxRequestsPerConnection: Int = 32768,
                                   remoteMaxRequestsPerConnection: Int =  8192,
                                   heartBeatIntervalSeconds: Int = 10,
                                   consistencyLevel: ConsistencyLevel = ConsistencyLevel.ONE)