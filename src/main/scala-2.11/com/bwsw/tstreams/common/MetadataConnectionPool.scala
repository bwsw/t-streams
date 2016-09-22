package com.bwsw.tstreams.common

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

import com.datastax.driver.core.Cluster.Builder
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy
import com.datastax.driver.core._
import org.slf4j.LoggerFactory

/**
  * singleton object which maintains shared C* session and cluster objects
  * to follow C* best practices: http://www.datastax.com/dev/blog/4-simple-rules-when-using-the-datastax-drivers-for-cassandra
  * Created by Ivan Kudryavtsev on 28.07.16.
  */
object MetadataConnectionPool {

  private val lock = new ReentrantLock(true)
  private val logger = LoggerFactory.getLogger(this.getClass)


  private var isClosed = false
  /**
    * Map for store clusters which are already created
    */
  private val clusterMap = scala.collection.mutable.Map[Set[InetSocketAddress], Cluster]()

  /**
    * Map for store Storage sessions which are already created
    */
  private val sessionMap = scala.collection.mutable.Map[(Set[InetSocketAddress], String), Session]()

  /**
    * Allows to get Cluster object or create new if no such cluster object was created.
    *
    * @param conf
    * @return
    */
  def getCluster(conf: CassandraConnectorConf): Cluster = {
    LockUtil.withLockOrDieDo[Cluster](lock, (10, TimeUnit.SECONDS), Some(logger), () => {
      if (isClosed) {
        throw new IllegalStateException("CassandraPool is closed. This is the illegal usage of the object.")
      }

      logger.info("start MetadataStorage instance creation")

      val cluster = {
        if (clusterMap.contains(conf.hosts)) {
          if (clusterMap(conf.hosts).isClosed) {
            clusterMap.remove(conf.hosts)
            getCluster(conf)
          }
          else
            clusterMap(conf.hosts)
        }
        else {
          val builder: Builder = Cluster.builder()
          if (conf.login != null && conf.password != null)
            builder.withCredentials(conf.login, conf.password)

          val options = new SocketOptions()
            .setConnectTimeoutMillis(conf.connectTimeoutMillis)
            .setReadTimeoutMillis(conf.readTimeoutMillis)

          val poolingOptions = new PoolingOptions()

          poolingOptions
            .setConnectionsPerHost(HostDistance.LOCAL, conf.localCoreConnectionsPerHost, conf.localMaxConnectionsPerHost)
            .setConnectionsPerHost(HostDistance.REMOTE, conf.remoteCoreConnectionsPerHost, conf.remoteMaxConnectionsPerHost)
            .setHeartbeatIntervalSeconds(conf.heartBeatIntervalSeconds)
            .setMaxRequestsPerConnection(HostDistance.LOCAL, conf.localMaxRequestsPerConnection)
            .setMaxRequestsPerConnection(HostDistance.REMOTE, conf.remoteMaxRequestsPerConnection)

          builder.addContactPointsWithPorts(conf.hosts.toSeq: _*)
            .withRetryPolicy(new MultipleRetryPolicy(conf.queryRetryCount))
            .withReconnectionPolicy(new ExponentialReconnectionPolicy(conf.minReconnectionDelayMillis, conf.maxReconnectionDelayMillis))
            .withLoadBalancingPolicy(new LocalNodeFirstLoadBalancingPolicy(conf.hosts, conf.localDC))
            .withSocketOptions(options)
            .withPoolingOptions(poolingOptions)
            .withQueryOptions(
              new QueryOptions()
                .setRefreshNodeIntervalMillis(0)
                .setRefreshNodeListIntervalMillis(0)
                .setRefreshSchemaIntervalMillis(0)
                .setConsistencyLevel(conf.consistencyLevel))
          val cluster = builder.build()
          clusterMap(conf.hosts) = cluster
          cluster
        }
      }
      cluster
    })
  }


  /**
    * Allows to get new or reuse session object for specified C* connection arguments
    *
    * @param conf
    * @param keyspace
    * @return
    */
  def getSession(conf: CassandraConnectorConf, keyspace: String): Session = {
    LockUtil.withLockOrDieDo[Session](lock, (10, TimeUnit.SECONDS), Some(logger), () => {
      if (isClosed) {
        throw new IllegalStateException("CassandraPool is closed. This is the illegal usage of the object.")
      }
      val session = {
        if (sessionMap.contains((conf.hosts, keyspace))) {
          sessionMap((conf.hosts, keyspace))
          if (sessionMap((conf.hosts, keyspace)).isClosed) {
            sessionMap.remove((conf.hosts, keyspace))
            getSession(conf, keyspace)
          }
          else
            sessionMap((conf.hosts, keyspace))
        }
        else {
          val session: Session = getCluster(conf).connect(keyspace)
          sessionMap((conf.hosts, keyspace)) = session
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
