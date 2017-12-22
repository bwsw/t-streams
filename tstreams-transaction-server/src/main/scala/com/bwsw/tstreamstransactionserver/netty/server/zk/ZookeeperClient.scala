package com.bwsw.tstreamstransactionserver.netty.server.zk

import java.io.Closeable
import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.exception.Throwable.ZkNoConnectionException
import com.bwsw.tstreamstransactionserver.netty.SocketHostPortPair
import com.bwsw.tstreamstransactionserver.netty.server.db.zk.ZookeeperStreamRepository
import org.apache.curator.RetryPolicy
import org.apache.curator.framework.CuratorFrameworkFactory

class ZookeeperClient(endpoints: String,
                      sessionTimeoutMillis: Int,
                      connectionTimeoutMillis: Int,
                      policy: RetryPolicy)
  extends Closeable {

  private[server] val client = {
    val connection = CuratorFrameworkFactory.builder()
      .sessionTimeoutMs(sessionTimeoutMillis)
      .connectionTimeoutMs(connectionTimeoutMillis)
      .retryPolicy(policy)
      .connectString(endpoints)
      .build()

    connection.start()
    val isConnected = connection
      .blockUntilConnected(
        connectionTimeoutMillis,
        TimeUnit.MILLISECONDS
      )

    if (isConnected)
      connection
    else
      throw new ZkNoConnectionException(endpoints)
  }


  def streamRepository(prefix: String): ZookeeperStreamRepository =
    new ZookeeperStreamRepository(client, prefix)

  def idGenerator(prefix: String): ZKIDGenerator =
    new ZKIDGenerator(client, policy, prefix)

  def masterElector(socket: SocketHostPortPair,
                    masterPrefix: String,
                    masterElectionPrefix: String): ZKMasterElector = {
    new ZKMasterElector(
      client,
      socket,
      masterPrefix,
      masterElectionPrefix
    )
  }

  override def equals(that: scala.Any): Boolean = that match {
    case that: ZookeeperClient =>
      this.client == that.client
    case _ =>
      false
  }

  override def hashCode(): Int = {
    31 * (
      31 * (
        31 * (
          31 * (
            31 + client.hashCode()
            ) + policy.hashCode()
          ) + endpoints.hashCode()
        ) + sessionTimeoutMillis.hashCode()
      ) + connectionTimeoutMillis.hashCode()
  }

  override def close(): Unit =
    scala.util.Try(client.close())
}
