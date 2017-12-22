package com.bwsw.tstreamstransactionserver.netty.server.zk

import java.util
import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreamstransactionserver.netty.SocketHostPortPair
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.leader.{LeaderLatch, LeaderLatchListener}
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.ZooDefs.{Ids, Perms}
import org.apache.zookeeper.data.ACL

import scala.util.Try

final class ZKMasterElector(curatorClient: CuratorFramework,
                            socket: SocketHostPortPair,
                            masterPrefix: String,
                            masterElectionPrefix: String)
  extends LeaderLatchListener {

  private val isStarted =
    new AtomicBoolean(false)

  private val leaderListeners =
    java.util.concurrent.ConcurrentHashMap.newKeySet[LeaderLatchListener]()

  private val leaderLatch =
    new LeaderLatch(
      curatorClient,
      masterElectionPrefix,
      socket.toString
    )
  leaderLatch.addListener(this)

  private def putSocketAddress(): Try[String] = {
    scala.util.Try(curatorClient.delete().forPath(masterPrefix))
    scala.util.Try {
      val permissions = new util.ArrayList[ACL]()
      permissions.add(new ACL(Perms.READ, Ids.ANYONE_ID_UNSAFE))
      curatorClient.create().creatingParentsIfNeeded()
        .withMode(CreateMode.EPHEMERAL)
        .withACL(permissions)
        .forPath(masterPrefix, socket.toString.getBytes())
    }
  }

  override def isLeader(): Unit = {
    putSocketAddress()
    leaderListeners.forEach(_.isLeader())
  }

  override def notLeader(): Unit = {
    leaderListeners.forEach(_.notLeader())
  }

  def addLeaderListener(listener: LeaderLatchListener): Unit = {
    leaderListeners.add(listener)
  }

  def removeLeaderListener(listener: LeaderLatchListener): Unit = {
    leaderListeners.remove(listener)
  }

  def leaderID: String =
    leaderLatch.getLeader.getId

  def start(): Unit = {
    val isNotStarted =
      isStarted.compareAndSet(false, true)

    if (isNotStarted)
      leaderLatch.start()
  }

  def stop(): Unit = {
    val started =
      isStarted.compareAndSet(true, false)

    if (started) {
      leaderLatch.close(
        LeaderLatch.CloseMode.SILENT
      )
    }
  }

  def hasLeadership(): Boolean =
    leaderLatch.hasLeadership
}
