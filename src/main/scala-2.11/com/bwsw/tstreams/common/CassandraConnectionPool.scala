package com.bwsw.tstreams.common

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

import com.datastax.driver.core.Cluster.Builder
import com.datastax.driver.core.{Cluster, Session}
import org.slf4j.LoggerFactory

/**
  * singleton object which maintains shared C* session and cluster objects
  * to follow C* best practices: http://www.datastax.com/dev/blog/4-simple-rules-when-using-the-datastax-drivers-for-cassandra
  * Created by ivan on 28.07.16.
  */
object CassandraConnectionPool {

  private val lock = new ReentrantLock(true)
  private val logger = LoggerFactory.getLogger(this.getClass)


  private var isClosed = false
  /**
    * Map for store clusters which are already created
    */
  private val clusterMap = scala.collection.mutable.Map[List[InetSocketAddress], Cluster]()

  /**
    * Map for store Storage sessions which are already created
    */
  private val sessionMap = scala.collection.mutable.Map[(List[InetSocketAddress], String), Session]()

  /**
    * Allows to get Cluster object or create new if no such cluster object was created.
    *
    * @param cassandraHosts
    * @param login
    * @param password
    * @return
    */
  def getCluster(cassandraHosts: List[InetSocketAddress], login: String = null, password: String = null): Cluster = {
    LockUtil.withLockOrDieDo[Cluster](lock, (10, TimeUnit.SECONDS), Some(logger), () => {
      if (isClosed) {
        throw new IllegalStateException("CassandraPool is closed. This is the illegal usage of the object.")
      }

      logger.info("start MetadataStorage instance creation")

      val sortedHosts = cassandraHosts.map(x => (x, x.hashCode())).sortBy(_._2).map(x => x._1)

      val cluster = {
        if (clusterMap.contains(sortedHosts))
          clusterMap(sortedHosts)
        else {
          val builder: Builder = Cluster.builder()
          if (login != null && password != null)
            builder.withCredentials(login, password)
          cassandraHosts.foreach(x => builder.addContactPointsWithPorts(x))
          val cluster = builder.build()
          clusterMap(sortedHosts) = cluster
          cluster
        }
      }
      cluster
    })
  }

  /**
    * Allows to get new or reuse session object for specified C* connection arguments
    *
    * @param cassandraHosts
    * @param keyspace
    * @param login
    * @param password
    * @return
    */
  def getSession(cassandraHosts: List[InetSocketAddress], keyspace: String, login: String = null, password: String = null): Session = {
    LockUtil.withLockOrDieDo[Session](lock, (10, TimeUnit.SECONDS), Some(logger), () => {
      if (isClosed) {
        throw new IllegalStateException("CassandraPool is closed. This is the illegal usage of the object.")
      }
      val sortedHosts = cassandraHosts.map(x => (x, x.hashCode())).sortBy(_._2).map(x => x._1)
      val session = {
        if (sessionMap.contains((sortedHosts, keyspace)))
          sessionMap((sortedHosts, keyspace))
        else {
          val session: Session = getCluster(cassandraHosts, login, password).connect(keyspace)
          sessionMap((sortedHosts, keyspace)) = session
          session
        }
      }
      session
    })
  }

  /**
    * finally stop work with Pool
    */
  def close() = {
    LockUtil.withLockOrDieDo[Unit](lock, (10, TimeUnit.SECONDS), Some(logger), () => {
      isClosed = true
      sessionMap.foreach(x => x._2.close())
      clusterMap.foreach(x => x._2.close()) //close all clusters for each instance
      clusterMap.clear()
    })
  }
}
