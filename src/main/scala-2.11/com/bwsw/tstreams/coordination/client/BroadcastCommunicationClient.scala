package com.bwsw.tstreams.coordination.client

import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreams.coordination.messages.state.TransactionStateMessage
import org.apache.curator.framework.CuratorFramework

import scala.collection.JavaConversions._


/**
  *
  * @param curatorClient
  * @param partitions
  */
class BroadcastCommunicationClient(curatorClient: CuratorFramework, partitions: Set[Int]) {

  val isStopped = new AtomicBoolean(true)

  private val partitionSubscribers = new java.util.concurrent.ConcurrentHashMap[Int, (Set[String], CommunicationClient)]()

  private val updateThread = new Thread(new Runnable {
    override def run(): Unit = {
      while(!isStopped.get()) {
        Thread.sleep(1000)
        partitions.foreach(p => updateSubscribers(p))
      }
    }
  })

  /**
    * Initialize coordinator
    */
  def init(): Unit = {
    isStopped.set(false)
    partitions.foreach { p => {
        partitionSubscribers.put(p, (Set[String]().empty, new CommunicationClient(10, 1, 0)))
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
  def publish(msg: TransactionStateMessage, onComplete: () => Unit): Unit = {
    if (!isStopped.get) {
      val (set, broadcaster) = partitionSubscribers.get(msg.partition)
      broadcaster.broadcast(set, msg)
    }
    onComplete()
  }

  /**
    * Update subscribers on specific partition
    */
  private def updateSubscribers(partition: Int) = {
    val (oldPeers, broadcaster) = partitionSubscribers.get(partition)
    if(curatorClient.checkExists.forPath(s"/subscribers/${partition}") != null) {
      val newPeers = curatorClient.getChildren.forPath(s"/subscribers/${partition}").toSet ++ oldPeers
      partitionSubscribers.put(partition, (newPeers, broadcaster))
    }
  }

  /**
    * Stop this Subscriber client
    */
  def stop() = {
    if (isStopped.getAndSet(true))
      throw new IllegalStateException("Producer->Subscriber notifier was stopped second time.")

    updateThread.join()

    partitions foreach { p => partitionSubscribers.get(p)._2.close() }
    partitionSubscribers.clear()
  }
}
