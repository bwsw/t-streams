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
                zkSessionTimeoutMs: Int,
                zkConnectionTimeoutMs: Int,
                zkRetryDelayMs: Int,
                zkRetryCount: Int) = this.synchronized {

    if (isInitialized.getAndSet(true))
      throw new IllegalStateException("Failed to initialize object as it's already initialized.")

    this.agentAddress = agentAddress
    this.stream = stream
    this.partitions = partitions

    val namespace = java.nio.file.Paths.get(zkRootPath, stream).toString.substring(1)
    curatorClient = CuratorFrameworkFactory.builder()
      .namespace(namespace)
      .connectionTimeoutMs(zkConnectionTimeoutMs)
      .sessionTimeoutMs(zkSessionTimeoutMs)
      .retryPolicy(new ExponentialBackoffRetry(zkRetryDelayMs, zkRetryCount))
      .connectString(zkHosts).build()

    curatorClient.start()

    initializeState()
  }

  /**
    * shuts down coordinator
    */
  def shutdown() = {
    if (!isInitialized.getAndSet(false))
      throw new IllegalStateException("Failed to stop object as it's already stopped.")

    partitions.foreach(p =>
      curatorClient.delete().forPath(s"/subscribers/$p/$agentAddress"))

    curatorClient.close()
  }

  /**
    * Try remove this subscriber if it was already created
    *
    */
  private def initializeState(): Unit = {
    partitions.foreach(p =>
      curatorClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(s"/subscribers/$p/$agentAddress"))
  }

}
