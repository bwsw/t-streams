package com.bwsw.tstreams.coordination.clients

import java.net.InetSocketAddress

import com.bwsw.tstreams.common.ZookeeperDLMService
import com.bwsw.tstreams.coordination.clients.publisher.SubscriberBroadcastNotifier
import com.bwsw.tstreams.coordination.messages.state.Message
import org.apache.zookeeper.{WatchedEvent, Watcher}


/**
  * Producer coordinator
  *
  * @param prefix           Zookeeper root prefix for all metadata
  * @param streamName       Producer stream
  * @param usedPartitions   Producer used partition
  * @param zkHosts          Zookeeper hosts to connect
  * @param zkSessionTimeout Zookeeper connect timeout
  */
class ProducerToSubscriberNotifier(prefix: String,
                                   streamName: String,
                                   usedPartitions: List[Int],
                                   zkHosts: List[InetSocketAddress],
                                   zkSessionTimeout: Int,
                                   zkConnectionTimeout: Int) {

  private val zkService = new ZookeeperDLMService(prefix, zkHosts, zkSessionTimeout, zkConnectionTimeout)
  private val broadcaster = new SubscriberBroadcastNotifier

  /**
    * Initialize coordinator
    */
  def init(): Unit = {
    usedPartitions foreach { p =>
      val watcher = new Watcher {
        override def process(event: WatchedEvent): Unit = {
          updateSubscribers(p)
          zkService.setWatcher(s"/subscribers/event/$streamName/$p", this)
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
  def publish(msg: Message, onComplete: () => Unit) = {
    broadcaster.broadcast(msg, onComplete)
  }

  /**
    * Update subscribers on specific partition
    */
  private def updateSubscribers(partition: Int) = {
    val subscribersPathOpt = zkService.getAllSubNodesData[String](s"/subscribers/agents/$streamName/$partition")
    if (subscribersPathOpt.isDefined)
      broadcaster.updateSubscribers(subscribersPathOpt.get)
  }

  /**
    * Get global distributed lock on stream
    *
    * @param streamName Stream name
    * @return com.twitter.common.zookeeper.DistributedLockImpl
    */
  def getStreamLock(streamName: String) = {
    val lock = zkService.getLock(s"/global/stream/$streamName")
    lock
  }

  /**
    * Stop this Subscriber client
    */
  def stop() = {
    broadcaster.close()
    zkService.close()
  }
}
