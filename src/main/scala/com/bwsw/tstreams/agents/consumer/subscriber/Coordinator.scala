package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreams.streams.Stream
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode


/**
  * Created by Ivan Kudryavtsev on 23.08.16.
  */
class Coordinator() {

  var agentAddress: String = null
  var curatorClient: CuratorFramework = null
  var partitions: Set[Int] = null
  var namespace: String = null

  val isInitialized = new AtomicBoolean(false)

  def bootstrap(stream: Stream,
                agentAddress: String,
                partitions: Set[Int]) = this.synchronized {

    if (isInitialized.getAndSet(true))
      throw new IllegalStateException("Failed to initialize object as it's already initialized.")

    this.agentAddress = agentAddress
    this.partitions = partitions
    this.namespace = stream.path
    this.curatorClient = stream.curator
    initializeState()
  }

  /**
    * shuts down coordinator
    */
  def shutdown() = {
    if (!isInitialized.getAndSet(false))
      throw new IllegalStateException("Failed to stop object as it's already stopped.")

    partitions.foreach(partition =>
      curatorClient.delete().forPath(s"$namespace/subscribers/$partition/$agentAddress"))
  }

  /**
    * Try remove this subscriber if it was already created
    *
    */
  private def initializeState(): Unit = {
    partitions.foreach(partition =>
      curatorClient.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(s"$namespace/subscribers/$partition/$agentAddress"))
  }

}
