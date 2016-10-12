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
        partitions.foreach(p => updateSubscribers(p))
        Thread.sleep(1000)
      }
    }
  })

  /**
    * Initialize coordinator
    */
  def init(): Unit = {
    updateThread.start()
    isStopped.set(false)
  }

  /**
    * Publish Message to all accepted subscribers
    *
    * @param msg Message
    */
  def publish(msg: TransactionStateMessage, onComplete: () => Unit): Unit = {
    if (!isStopped.get) {
      val (set, broadcaster) = partitionSubscribers.get(msg.partition)
      partitionSubscribers.put(msg.partition, (broadcaster.broadcast(set, msg), broadcaster))
    }
    onComplete()
  }

  /**
    * Update subscribers on specific partition
    */
  private def updateSubscribers(partition: Int) = {
    if (!isStopped.get) {
      val (_, broadcaster) = partitionSubscribers.get(partition)
      val children = curatorClient.getChildren.forPath(s"subscribers/${partition}").toSet
      broadcaster.initConnections(children)
      partitionSubscribers.put(partition, (children, broadcaster))
    }

  }

  /**
    * Stop this Subscriber client
    */
  def stop() = {
    if (isStopped.getAndSet(true))
      throw new IllegalStateException("Producer->Subscriber notifier was stopped second time.")

    partitions foreach { p => partitionSubscribers.get(p)._2.close() }
    partitionSubscribers.clear()
  }
}
