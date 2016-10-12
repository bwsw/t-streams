package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.concurrent.atomic.AtomicBoolean

import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.zookeeper.CreateMode

/**
  * Created by Ivan Kudryavtsev on 23.08.16.
  */
class Coordinator() {

  var agentAddress: String = null
  var stream: String = null
  var curatorClient: CuratorFramework = null
  var partitions: Set[Int] = null


  val isInitialized = new AtomicBoolean(false)

  def bootstrap(agentAddress: String,
                stream: String,
                partitions: Set[Int],
                zkRootPath: String,
                zkHosts: String,
                zkSessionTimeout: Int,
                zkConnectionTimeout: Int) = this.synchronized {

    if (isInitialized.getAndSet(true))
      throw new IllegalStateException("Failed to initialize object as it's already initialized.")

    this.agentAddress = agentAddress
    this.stream = stream
    this.partitions = partitions

    curatorClient = CuratorFrameworkFactory.builder()
      .namespace(java.nio.file.Paths.get(zkRootPath, stream).toString)
      .connectionTimeoutMs(zkConnectionTimeout * 1000)
      .sessionTimeoutMs(zkSessionTimeout * 1000)
      .retryPolicy(new ExponentialBackoffRetry(1000, 3))
      .connectString(zkHosts).build()

    curatorClient.start()
    curatorClient.create().creatingParentContainersIfNeeded().forPath("subscribers")

    initializeState()
  }

  /**
    * shuts down coordinator
    */
  def shutdown() = {
    if (!isInitialized.getAndSet(false))
      throw new IllegalStateException("Failed to stop object as it's already stopped.")

    curatorClient.close()
  }

  /**
    * Try remove this subscriber if it was already created
    *
    */
  private def initializeState(): Unit = {
    curatorClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(agentAddress)
  }

  private def getSubscriberEventPath(p: Int) = s"/subscribers/event/$stream/$p"

  private def getSubscriberMembershipPath(p: Int) = s"/subscribers/agents/$stream/$p/$agentAddress"
}
