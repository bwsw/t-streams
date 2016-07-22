package com.bwsw.tstreams.data.cassandra

import java.net.InetSocketAddress
import java.util.concurrent.locks.ReentrantLock
import com.datastax.driver.core.Cluster.Builder
import com.datastax.driver.core.{Cluster, Session}


/**
 * Factory for creating cassandra storage instances
 */
class CassandraStorageFactory {

  /**
   * Map for memorize clusters which are already created
   */
  private val clusterMap = scala.collection.mutable.Map[List[InetSocketAddress], Cluster]()

  /**
   * Map for memorize sessions which are already created
   */
  private val sessionMap = scala.collection.mutable.Map[(List[InetSocketAddress], String), Session]()

  /**
   * Lock for providing getInstance thread safeness
   */
  private val lock = new ReentrantLock(true)

  /**
   *
   * @param cassandraStorageOptions Cassandra client options
   * @return Instance of CassandraStorage
   */
  def getInstance(cassandraStorageOptions: CassandraStorageOptions) : CassandraStorage = {
    lock.lock()

    val sortedHosts = cassandraStorageOptions.cassandraHosts.map(x=>(x,x.hashCode())).sortBy(_._2).map(x=>x._1)

    val cluster = {
      if (clusterMap.contains(sortedHosts))
        clusterMap(sortedHosts)
      else{
        val builder: Builder = Cluster.builder()

        if (cassandraStorageOptions.login != null && cassandraStorageOptions.password != null)
          builder.withCredentials(cassandraStorageOptions.login, cassandraStorageOptions.password)

        cassandraStorageOptions.cassandraHosts.foreach(x => builder.addContactPointsWithPorts(x))
        val cluster = builder.build()
        clusterMap(sortedHosts) = cluster
        cluster
      }
    }

    val session = {
      if (sessionMap.contains((sortedHosts,cassandraStorageOptions.keyspace)))
        sessionMap((sortedHosts,cassandraStorageOptions.keyspace))
      else{
        val session: Session = cluster.connect(cassandraStorageOptions.keyspace)
        sessionMap((sortedHosts, cassandraStorageOptions.keyspace)) = session
        session
      }
    }

    val inst = new CassandraStorage(cluster, session, cassandraStorageOptions.keyspace)
    lock.unlock()

    inst
  }

  /**
   * Close all factory storage instances
   */
  def closeFactory() : Unit = {
    clusterMap.foreach{x=>x._2.close()}
    sessionMap.foreach{x=>x._2.close()}
    clusterMap.clear()
    sessionMap.clear()
  }
}
