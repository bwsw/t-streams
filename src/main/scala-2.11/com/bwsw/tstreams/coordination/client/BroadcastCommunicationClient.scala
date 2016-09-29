package com.bwsw.tstreams.coordination.client

import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreams.common.ZookeeperDLMService
import com.bwsw.tstreams.coordination.messages.state.TransactionStateMessage
import com.bwsw.tstreams.coordination.producer.AgentsStateDBService
import org.apache.zookeeper.{WatchedEvent, Watcher}


/**
  *
  * @param agentsStateManager
  * @param usedPartitions
  */
class BroadcastCommunicationClient(agentsStateManager: AgentsStateDBService,
                                   usedPartitions: List[Int]) {

  val isStopped = new AtomicBoolean(false)

  private val partitionSubscribers = new java.util.concurrent.ConcurrentHashMap[Int, (Set[String], CommunicationClient)]()

  /**
    * Initialize coordinator
    */
  def init(): Unit = {
    agentsStateManager.doLocked { initInternal() }
  }

  /**
    * init itself
    */
  def initInternal(): Unit = {
    usedPartitions foreach { p =>
      partitionSubscribers
        .put(p, (Set[String]().empty, new CommunicationClient(10, 1, 0)))
      val watcher = new Watcher {
        override def process(event: WatchedEvent): Unit = {
          val wo = this
          ZookeeperDLMService.executor.submit("<UpdateSubscribersTask>", new Runnable {
            override def run(): Unit = {
              updateSubscribers(p)
              agentsStateManager.setSubscriberStateWatcher(p, wo)
            }
          })
        }
      }
      watcher.process(null)
    }
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
      val endpoints = agentsStateManager.getPartitionSubscribers(partition)
      broadcaster.initConnections(endpoints)
      partitionSubscribers.put(partition, (agentsStateManager.getPartitionSubscribers(partition), broadcaster))
    }

  }

  /**
    * Stop this Subscriber client
    */
  def stop() = {
    if (isStopped.getAndSet(true))
      throw new IllegalStateException("Producer->Subscriber notifier was stopped second time.")

    usedPartitions foreach { p => partitionSubscribers.get(p)._2.close() }
    partitionSubscribers.clear()
  }
}
