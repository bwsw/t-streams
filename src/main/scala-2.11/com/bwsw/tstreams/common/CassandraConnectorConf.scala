package com.bwsw.tstreams.common

import java.net.InetSocketAddress

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
                                   password: String = null
                                 )