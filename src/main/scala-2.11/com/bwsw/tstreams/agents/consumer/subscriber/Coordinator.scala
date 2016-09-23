package com.bwsw.tstreams.agents.consumer.subscriber

import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreams.common.{LockUtil, ZookeeperDLMService}
import org.apache.zookeeper.{CreateMode, KeeperException}

/**
  * Created by Ivan Kudryavtsev on 23.08.16.
  */
class Coordinator() {

  var agentAddress: String = null
  var stream: String = null
  var dlm: ZookeeperDLMService = null
  var partitions: Set[Int] = null


  val isInitialized = new AtomicBoolean(false)

  def bootstrap(agentAddress: String,
                stream: String,
                partitions: Set[Int],
                zkRootPath: String,
                zkHosts: Set[InetSocketAddress],
                zkSessionTimeout: Int,
                zkConnectionTimeout: Int) = this.synchronized {

    if (isInitialized.getAndSet(true))
      throw new IllegalStateException("Failed to initialize object as it's already initialized.")

    this.agentAddress = agentAddress
    this.stream = stream
    this.partitions = partitions

    dlm = new ZookeeperDLMService(zkRootPath, zkHosts.toList, zkSessionTimeout, zkConnectionTimeout)

    initializeState()
  }

  /**
    * shuts down coordinator
    */
  def shutdown() = {
    if (!isInitialized.getAndSet(false))
      throw new IllegalStateException("Failed to stop object as it's already stopped.")
    partitions foreach (p => dlm.delete(getSubscriberMembershipPath(p)))
    dlm.close()
  }

  /**
    * Try remove this subscriber if it was already created
    *
    */
  private def initializeState(): Unit = {
    LockUtil.withZkLockOrDieDo[Unit](dlm.getLock(ZookeeperDLMService.CREATE_PATH_LOCK), (100, TimeUnit.SECONDS), Some(ZookeeperDLMService.logger), {
      partitions foreach (p => {
        try {
          if (!dlm.exist(getSubscriberEventPath(p)))
            dlm.create[String](getSubscriberEventPath(p), s"$stream/$p", CreateMode.PERSISTENT)
        } catch {
          case e: KeeperException =>
          case e: IllegalStateException =>
        }

        try {
          dlm.delete(getSubscriberMembershipPath(p))
        } catch {
          case e: KeeperException =>
        }
        dlm.create(getSubscriberMembershipPath(p), agentAddress, CreateMode.EPHEMERAL)
      })
    })

    partitions foreach (p => dlm.notify(getSubscriberEventPath(p)))

  }

  private def getSubscriberEventPath(p: Int) = s"/subscribers/event/$stream/$p"

  private def getSubscriberMembershipPath(p: Int) = s"/subscribers/agents/$stream/$p/$agentAddress"
}
