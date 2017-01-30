package com.bwsw.tstreams.common

import com.bwsw.tstreamstransactionserver.configProperties.{ClientConfig, ConfigMap}
import com.bwsw.tstreamstransactionserver.netty.client.Client


/**
  * Created by Ivan Kudryavtsev on 28.01.17.
  */

/**
  *
  * @param login
  * @param password
  * @param connectionTimeoutMs
  * @param retryDelayMs
  * @param tokenRetryDelayMs
  * @param tokenConnectionTimeoutMs
  */
case class StorageClientAuthOptions(login: String = "", password: String = "", connectionTimeoutMs: Int = 5000, retryDelayMs: Int = 500,
                                    tokenRetryDelayMs: Int = 200, tokenConnectionTimeoutMs: Int = 5000)

/**
  *
  * @param endpoints
  * @param prefix
  * @param sessionTimeoutMs
  * @param retryCount
  * @param retryDelayMs
  * @param connectionTimeoutMs
  */
case class StorageClientZookeeperOptions(endpoints: String = "127.0.0.1:2181", prefix: String = "/tts", sessionTimeoutMs: Int = 10000, retryCount: Int = 5,
                                         retryDelayMs: Int = 500, connectionTimeoutMs: Int = 10000)

/**
  *
  * @param connectionTimeoutMs
  * @param retryDelayMs
  * @param threadPool
  */
case class StorageClientOptions(connectionTimeoutMs: Int = 5000, retryDelayMs: Int = 200, threadPool: Int = 4)

/**
  *
  * @param authOpts
  * @param zookeeperOpts
  * @param clientOpts
  */
class StorageClientBuilder private(authOpts: StorageClientAuthOptions, zookeeperOpts: StorageClientZookeeperOptions, clientOpts: StorageClientOptions) {
  private val authOptions       = authOpts
  private val zookeeperOptions  = zookeeperOpts
  private val clientOptions     = clientOpts

  def this() = this(StorageClientAuthOptions(), StorageClientZookeeperOptions(), StorageClientOptions())

  def withAuthOptions(authOptions: StorageClientAuthOptions) = new StorageClientBuilder(authOptions, zookeeperOptions, clientOptions)

  def withZookeeperOptions(zookeeperOptions: StorageClientZookeeperOptions) = new StorageClientBuilder(authOptions, zookeeperOptions, clientOptions)

  def withClientOptions(clientOptions: StorageClientOptions) = new StorageClientBuilder(authOptions, zookeeperOptions, clientOptions)

  def build() = new StorageClient(clientOptions, authOptions, zookeeperOptions)

  def getClientOptions()    = clientOptions.copy()
  def getZookeeperOptions() = zookeeperOptions.copy()
  def getAuthOptions()      = authOptions.copy()
}

/**
  *
  * @param clientOptions
  * @param authOptions
  * @param zookeeperOptions
  */
class StorageClient private(clientOptions: StorageClientOptions, authOptions: StorageClientAuthOptions, zookeeperOptions: StorageClientZookeeperOptions) {
  private val map = scala.collection.mutable.Map[String,String]()

  map += (("server.timeout.connection",         clientOptions.connectionTimeoutMs.toString))
  map += (("server.timeout.betweenRetries",     clientOptions.retryDelayMs.toString))
  map += (("client.pool",                       clientOptions.threadPool.toString))

  map += (("auth.login",                        authOptions.login))
  map += (("auth.password",                     authOptions.password))
  map += (("auth.timeout.connection",           authOptions.connectionTimeoutMs.toString))
  map += (("auth.timeout.betweenRetries",       authOptions.retryDelayMs.toString))

  map += (("auth.token.timeout.betweenRetries", authOptions.tokenRetryDelayMs.toString))
  map += (("auth.token.timeout.connection",     authOptions.tokenConnectionTimeoutMs.toString))

  map += (("zk.endpoints",                      zookeeperOptions.endpoints))
  map += (("zk.prefix",                         zookeeperOptions.prefix))
  map += (("zk.retries.max",                    zookeeperOptions.retryCount.toString))
  map += (("zk.timeout.session",                zookeeperOptions.sessionTimeoutMs.toString))
  map += (("zk.timeout.betweenRetries",         zookeeperOptions.retryDelayMs.toString))
  map += (("zk.timeout.connection",             zookeeperOptions.connectionTimeoutMs.toString))

  val client = new Client(new ClientConfig(new ConfigMap(map.toMap)))

}
