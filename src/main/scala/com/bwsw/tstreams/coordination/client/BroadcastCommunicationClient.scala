package com.bwsw.tstreams.coordination.client

import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreams.coordination.messages.state.TransactionStateMessage
import org.apache.curator.framework.CuratorFramework

import scala.collection.JavaConverters._


/**
  *
  * @param curatorClient
  * @param partitions
  */
class BroadcastCommunicationClient(curatorClient: CuratorFramework, partitions: Set[Int]) {

  private val UPDATE_PERIOD_MS = 1000

  private val isStopped = new AtomicBoolean(true)
  private val communicationClient = new CommunicationClient(10, 1, 0)

  private val partitionSubscribers = new java.util.concurrent.ConcurrentHashMap[Int, Set[String]]()

  private val updateThread = new Thread(() => {
    while (!isStopped.get()) {
      Thread.sleep(UPDATE_PERIOD_MS)
      partitions.foreach(p => updateSubscribers(p))
    }
  })

  /**
    * Initialize coordinator
    */
  def init(): Unit = {
    isStopped.set(false)
    partitions.foreach { p => {
      partitionSubscribers.put(p, Set[String]().empty)
      updateSubscribers(p)
    }
    }
    updateThread.start()
  }

  /**
    * Publish Message to all accepted subscribers
    *
    * @param msg Message
    */
  def publish(msg: TransactionStateMessage): Unit = {
    if (!isStopped.get) {
      val set = partitionSubscribers.get(msg.partition)
      communicationClient.broadcast(set, msg)
    }
  }

  /**
    * Update subscribers on specific partition
    */
  private def updateSubscribers(partition: Int) = {
    val oldPeers = partitionSubscribers.get(partition)
    if (curatorClient.checkExists.forPath(s"/subscribers/$partition") != null) {
      val newPeers = curatorClient.getChildren.forPath(s"/subscribers/$partition").asScala.toSet ++ oldPeers
      partitionSubscribers.put(partition, newPeers)
    }
  }

  /**
    * Stop this Subscriber client
    */
  def stop() = {
    if (isStopped.getAndSet(true))
      throw new IllegalStateException("Producer->Subscriber notifier was stopped second time.")
    updateThread.join()
    communicationClient.close()
    partitionSubscribers.clear()
  }
}
