package com.bwsw.tstreams.common

import com.bwsw.tstreamstransactionserver.configProperties.{ClientConfig, ConfigMap}
import com.bwsw.tstreamstransactionserver.netty.client.Client

case class StorageClientAuthOptions(login: String, password: String, connectionTimeoutMs: Int = 5000, timeoutBetweenRetriesMs: Int = 500,
                                    tokenTimeoutBetweenRetriesMs: Int = 200, tokenTimeoutConnectionMs: Int = 5000)

case class StorageClientZookeeperOptions(endpoints: String, prefix: String = "/tts", sessionTimeoutMs: Int = 10000, retriesMax: Int = 5,
                                         timeoutBetweenRetriesMs: Int = 500, connectionTimeoutMs: Int = 10000)

case class StorageClientOptions(connectionTimeoutMs: Int = 5000, timeoutBetweenRetriesMs: Int = 200, threadPool: Int = 4)

/**
  * Created by Ivan Kudryavtsev on 28.01.17.
  */
class StorageClient(clientOptions: StorageClientOptions, authOptions: StorageClientAuthOptions, zookeeperOptions: StorageClientZookeeperOptions) {
  private val map = scala.collection.mutable.Map[String,String]()

  map += (("server.timeout.connection",         clientOptions.connectionTimeoutMs.toString))
  map += (("server.timeout.betweenRetries",     clientOptions.timeoutBetweenRetriesMs.toString))
  map += (("client.pool",                       clientOptions.threadPool.toString))

  map += (("auth.login",                        authOptions.login))
  map += (("auth.password",                     authOptions.password))

  map += (("auth.timeout.connection",           authOptions.connectionTimeoutMs.toString))
  map += (("auth.timeout.betweenRetries",       authOptions.timeoutBetweenRetriesMs.toString))

  map += (("auth.token.timeout.betweenRetries", authOptions.tokenTimeoutBetweenRetriesMs.toString))
  map += (("auth.token.timeout.connection",     authOptions.tokenTimeoutConnectionMs.toString))

  map += (("zk.endpoints",                      zookeeperOptions.endpoints))
  map += (("zk.prefix",                         zookeeperOptions.prefix))
  map += (("zk.retries.max",                    zookeeperOptions.retriesMax.toString))
  map += (("zk.timeout.session",                zookeeperOptions.sessionTimeoutMs.toString))
  map += (("zk.timeout.betweenRetries",         zookeeperOptions.timeoutBetweenRetriesMs.toString))
  map += (("zk.timeout.connection",             zookeeperOptions.connectionTimeoutMs.toString))

  val client = new Client(new ClientConfig(new ConfigMap(map.toMap)))

}
