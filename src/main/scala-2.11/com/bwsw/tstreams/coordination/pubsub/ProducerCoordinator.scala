package com.bwsw.tstreams.coordination.pubsub

import java.net.InetSocketAddress
import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.common.zkservice.ZkService
import com.bwsw.tstreams.coordination.pubsub.publisher.Broadcaster
import com.bwsw.tstreams.coordination.pubsub.messages.ProducerTopicMessage
import org.apache.zookeeper.{WatchedEvent, Watcher}


/**
 * Producer coordinator
 * @param prefix Zookeeper root prefix for all metadata
 * @param streamName Producer stream
 * @param usedPartitions Producer used partition
 * @param zkHosts Zookeeper hosts to connect
 * @param zkSessionTimeout Zookeeper connect timeout
 */
class ProducerCoordinator(prefix : String,
                          streamName : String,
                          usedPartitions : List[Int],
                          zkHosts : List[InetSocketAddress],
                          zkSessionTimeout : Int,
                          zkConnectionTimeout : Int) {
  private val zkService = new ZkService(prefix, zkHosts, zkSessionTimeout, zkConnectionTimeout)
  private val broadcaster = new Broadcaster
  private val lock = new ReentrantLock(true)

  /**
   * Initialize coordinator
   */
  def init() : Unit = {
    usedPartitions foreach { p =>
      val watcher = new Watcher {
        override def process(event: WatchedEvent): Unit = {
          updateSubscribers(p)
          zkService.setWatcher(s"/subscribers/event/$streamName/$p", this)
        }
      }
      zkService.setWatcher(s"/subscribers/event/$streamName/$p", watcher)
      updateSubscribers(p)
    }
  }

  /**
   * Publish [[ProducerTopicMessage]]] to all accepted subscribers
   * @param msg [[ProducerTopicMessage]]]
   */
  def publish(msg : ProducerTopicMessage) = {
    lock.lock()
    broadcaster.broadcast(msg)
    lock.unlock()
  }

  /**
   * Update subscribers on specific partition
   */
  private def updateSubscribers(partition : Int) = {
    lock.lock()
    val subscribersPathOpt = zkService.getAllSubNodesData[String](s"/subscribers/agents/$streamName/$partition")
    if (subscribersPathOpt.isDefined)
      broadcaster.updateSubscribers(subscribersPathOpt.get)
    lock.unlock()
  }

  /**
   * Get global distributed lock on stream
   * @param streamName Stream name
   * @return [[com.twitter.common.zookeeper.DistributedLockImpl]]]
   */
  def getStreamLock(streamName : String)  = {
    val lock = zkService.getLock(s"/global/stream/$streamName")
    lock
  }

  /**
   * Stop this coordinator
   */
  def stop() = {
    broadcaster.close()
    zkService.close()
  }
}
