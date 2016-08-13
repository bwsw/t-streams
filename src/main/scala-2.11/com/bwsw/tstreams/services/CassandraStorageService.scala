package com.bwsw.tstreams.services

import java.net.InetSocketAddress

import com.datastax.driver.core.Cluster.Builder
import com.datastax.driver.core.{Cluster, Session}
import org.slf4j.LoggerFactory


/**
  * Service for metadata storage. Include static methods for cluster initialization,
  * creating new keyspace, dropping keyspace
  */
object CassandraStorageService {

  /**
    * MetadataStorageServiceLogger for logging
    */
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Creates keyspace for Metadata.
    *
    * @param cassandraHosts      Ring hosts to join
    * @param keyspace            Keyspace to create
    * @param replicationStrategy One of supported C* strategies (enum CassandraStrategies.CassandraStrategies)
    * @param replicationFactor   How to replicate data. Default = 1
    */
  def createKeyspace(cassandraHosts: Set[String],
                     keyspace: String,
                     replicationStrategy: CassandraStrategies.CassandraStrategies,
                     replicationFactor: Int = 1) = {
    logger.info(s"Create keyspace: $keyspace")

    val cluster = getCluster(cassandraHosts)
    val session: Session = cluster.connect()

    logger.debug(s"Execute create statement for keyspace:{$keyspace}")

    session.execute(s"CREATE KEYSPACE IF NOT EXISTS $keyspace WITH replication = " +
      s" {'class': '$replicationStrategy', 'replication_factor': '$replicationFactor'} " +
      s" AND durable_writes = true")

    session.close()
    cluster.close()

    logger.info(s"End create keyspace: $keyspace")
  }

  /**
    * Internal method to create cluster without session
    *
    * @param hosts Hosts connect to
    * @return Cluster connected to hosts
    */
  private def getCluster(hosts: Set[String]): Cluster = {
    logger.info(s"Start create cluster for hosts : {${hosts.mkString(",")}")
    val builder: Builder = Cluster.builder()
    hosts.foreach(x => builder.addContactPointsWithPorts(new InetSocketAddress(x.split(":").head, x.split(":").tail.head.toInt)))
    val cluster = builder.build()
    logger.info(s"End create cluster for hosts : {${hosts.mkString(",")}")
    cluster
  }

  /**
    * Drops keyspace for Metadata.
    *
    * @param cassandraHosts Ring hosts to join
    * @param keyspace       Keyspace to drop
    */
  def dropKeyspace(cassandraHosts: Set[String],
                   keyspace: String) = {

    val cluster = getCluster(cassandraHosts)
    val session: Session = cluster.connect()

    logger.debug(s"Start drop keyspace: $keyspace")
    session.execute(s"DROP KEYSPACE IF EXISTS $keyspace")

    session.close()
    cluster.close()

    logger.info(s"End drop keyspace:$keyspace")
  }

}

/**
  * Enumeration for accessing available network topologies
  */
object CassandraStrategies extends Enumeration {
  type CassandraStrategies = Value
  val SimpleStrategy = Value("SimpleStrategy")
  val NetworkTopologyStrategy = Value("NetworkTopologyStrategy")
}