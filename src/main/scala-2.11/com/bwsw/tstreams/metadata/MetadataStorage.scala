package com.bwsw.tstreams.metadata

import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.common.{CassandraConnectorConf, CassandraHelper, MetadataConnectionPool}
import com.datastax.driver.core._
import org.slf4j.LoggerFactory

/**
  * @param cluster  Cluster instance for metadata storage
  * @param session  Session instance for metadata storage
  * @param keyspace Keyspace for metadata storage
  */
class MetadataStorage(cluster: Cluster, session: Session, keyspace: String) {

  def getSession() = session
  /**
    * Uniq id for this MetadataStorage
    */
  val id = java.util.UUID.randomUUID().toString

  /**
    * MetadataStorage logger for logging
    */
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * @return Closed this storage or not
    */
  def isClosed: Boolean = session.isClosed // && cluster.isClosed TODO: check it

  /**
    * Removes MetadataStorage
    */
  def remove() = {
    CassandraHelper.dropMetadataTables(session, keyspace)
  }

  /**
    * Initializes metadata (creates tables in C*)
    */
  def init() = {
    CassandraHelper.createMetadataTables(session, keyspace)
  }

  /**
    * Truncates data in metadata storage
    */
  def truncate() = {
    CassandraHelper.clearMetadataTables(session, keyspace)
  }

}


/**
  * Factory for creating MetadataStorage instances
  */
class MetadataStorageFactory {
  /**
    * Lock for providing getInstance thread safeness
    */
  private val lock = new ReentrantLock(true)
  var isClosed = false
  /**
    * MetadataStorageFactory logger for logging
    */
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * stores metadata instances which shareable
    */
  private val metadataMap = scala.collection.mutable.Map[Session, MetadataStorage]()

  /**
    * Fabric method which returns new MetadataStorage
    *
    * @param conf     List of hosts to connect in C* cluster
    * @param keyspace Keyspace to use for metadata storage
    * @return Instance of MetadataStorage
    */
  def getInstance(conf: CassandraConnectorConf, keyspace: String): MetadataStorage = {
    lock.lock()

    if (isClosed)
      throw new IllegalStateException("TStreamsFactory is closed. This is the illegal usage of the object.")

    logger.info("start MetadataStorage instance creation")

    val session = MetadataConnectionPool.getSession(conf, keyspace)
    val metadataStorage = {
      if (metadataMap.contains(session))
        metadataMap(session)
      else {
        val m = new MetadataStorage(session.getCluster, session, keyspace)
        metadataMap(session) = m
        m
      }
    }
    logger.info("finished MetadataStorage instance creation")
    //val inst = new MetadataStorage(cluster,session,keyspace)
    lock.unlock()

    metadataStorage
  }

  /**
    * Closes all factory MetadataStorage instances
    */
  def closeFactory() = {
    lock.lock()

    if (isClosed)
      throw new IllegalStateException("MetadataStorageFactory is closed. This is repeatable close operation.")
    isClosed = true
    metadataMap.clear()
    lock.unlock()
  }
}

