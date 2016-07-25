package com.bwsw.tstreams.metadata

import java.net.InetSocketAddress
import java.util.concurrent.locks.ReentrantLock
import com.bwsw.tstreams.entities._
import com.datastax.driver.core.Cluster.Builder
import com.datastax.driver.core._
import org.slf4j.LoggerFactory


/**
 * @param cluster Cluster instance for metadata storage
 * @param session Session instance for metadata storage
 * @param keyspace Keyspace for metadata storage
 */
class MetadataStorage(cluster: Cluster, session: Session, keyspace: String) {

  /**
   * Uniq id for this MetadataStorage
   */
  val id = java.util.UUID.randomUUID().toString

  /**
   * MetadataStorage logger for logging
   */
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Stream entity instance
   */
  val streamEntity = new StreamEntity("streams", session)

  /**
   * Commit entity instance
   */
  val commitEntity = new CommitEntity("commit_log", session)

  ///**
  // * Commit entity instance for producer async txn update
  // */
  ////val producerCommitEntity = new CommitEntity("commit_log", session)

  /**
   * Consumer entity instance
   */
  val consumerEntity = new ConsumerEntity("consumers", session)

  /**
   * Group commit entity instance
   */
  lazy val groupCommitEntity = new GroupCommitEntity("consumers", "commit_log", session)

  /**
   * @return Closed this storage or not
   */
  def isClosed : Boolean = session.isClosed // && cluster.isClosed TODO: check it

  /**
    * Removes MetadataStorage
    */
  def remove() = {
    logger.info("start removing MetadataStorage tables from cassandra")

    logger.debug("dropping stream_commit_last table\n")
    session.execute("DROP TABLE stream_commit_last")

    logger.debug("dropping consumers table")
    session.execute("DROP TABLE consumers")

    logger.debug("dropping streams table")
    session.execute("DROP TABLE streams")

    logger.debug("dropping commit_log table")
    session.execute("DROP TABLE commit_log")

    logger.debug("dropping generators table")
    session.execute("DROP TABLE generators")

    logger.info("finished removing MetadataStorage tables from cassandra")
  }

  /**
    * Initializes metadata (creates tables in C*)
    */
  def init() = {
    logger.info("start initializing MetadataStorage tables")

    logger.debug("start creating stream_commit_last table")
    session.execute(s"CREATE TABLE stream_commit_last (" +
      s"stream text, " +
      s"partition int, " +
      s"transaction timeuuid, " +
      s"PRIMARY KEY (stream, partition))")

    logger.debug("start creating consumers table")
    session.execute(s"CREATE TABLE consumers (" +
      s"name text, " +
      s"stream text, " +
      s"partition int, " +
      s"last_transaction timeuuid, " +
      s"PRIMARY KEY (name, stream, partition))")

    logger.debug("start creating streams table")
    session.execute(s"CREATE TABLE streams (" +
      s"stream_name text PRIMARY KEY, " +
      s"partitions int, " +
      s"description text)")

    logger.debug("start creating commit log")
    session.execute(s"CREATE TABLE commit_log (" +
      s"stream text, " +
      s"partition int, " +
      s"transaction timeuuid, " +
      s"cnt int, " +
      s"PRIMARY KEY (stream, partition, transaction))")

    logger.debug("start creating generators table")
    session.execute(s"CREATE TABLE generators (" +
      s"name text, " +
      s"time timeuuid, " +
      s"PRIMARY KEY (name))")

    logger.info("finished initializing MetadataStorage tables")
  }

  /**
    * Truncates data in metadata storage
    */
  def truncate() = {
    logger.info("start truncating MetadataStorage tables")

    logger.debug("removing data from stream_commit_last table")
    session.execute("TRUNCATE stream_commit_last")

    logger.debug("removing data from consumers table")
    session.execute("TRUNCATE consumers")

    logger.debug("removing data from streams table")
    session.execute("TRUNCATE streams")

    logger.debug("removing data from commit_log table")
    session.execute("TRUNCATE commit_log")

    logger.debug("removing data from generators table")
    session.execute("TRUNCATE generators")

    logger.info("finished truncating MetadataStorage tables")
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
   * Map for memorize clusters which are already created
   */
  private val clusterMap = scala.collection.mutable.Map[List[InetSocketAddress], Cluster]()

  /**
   * Map for memorize Storage instances which are already created
   */
  private val instancesMap = scala.collection.mutable.Map[(List[InetSocketAddress], String), Session]()

  private val metadataMap = scala.collection.mutable.Map[Session, MetadataStorage]()

  /**
    * Fabric method which returns new MetadataStorage
    * @param cassandraHosts List of hosts to connect in C* cluster
    * @param keyspace Keyspace to use for metadata storage
    * @return Instance of MetadataStorage
    */
  def getInstance(cassandraHosts : List[InetSocketAddress], keyspace : String, login: String = null, password: String = null): MetadataStorage = {
    lock.lock()

    if(isClosed)
      throw new IllegalStateException("TStreamsFactory is closed. This is the illegal usage of the object.")

    logger.info("start MetadataStorage instance creation")

    val sortedHosts = cassandraHosts.map(x=>(x,x.hashCode())).sortBy(_._2).map(x=>x._1)

    val cluster = {
      if (clusterMap.contains(sortedHosts))
        clusterMap(sortedHosts)
      else{
        val builder: Builder = Cluster.builder()
        if (login != null && password != null)
          builder.withCredentials(login, password)
        cassandraHosts.foreach(x => builder.addContactPointsWithPorts(x))
        val cluster = builder.build()
        clusterMap(sortedHosts) = cluster
        cluster
      }
    }

    val session = {
      if (instancesMap.contains((sortedHosts,keyspace)))
        instancesMap((sortedHosts,keyspace))
      else {
        val session: Session = cluster.connect(keyspace)
        instancesMap((sortedHosts, keyspace)) = session
        session
      }
    }

    val metadataStorage = {
      if (metadataMap.contains(session))
        metadataMap(session)
      else {

        val m = new MetadataStorage(cluster, session, keyspace)
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

    if(isClosed)
      throw new IllegalStateException("MetadataStorageFactory is closed. This is repeatable close operation.")

    isClosed = true

    metadataMap.clear()
    instancesMap.foreach(x => x._2.close()) //close all sessions for each instance
    clusterMap.foreach(x => x._2.close()) //close all clusters for each instance
    clusterMap.clear()
    instancesMap.clear()
    lock.unlock()
  }
}

