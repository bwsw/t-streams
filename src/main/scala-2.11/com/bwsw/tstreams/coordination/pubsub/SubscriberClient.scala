package com.bwsw.tstreams.coordination.pubsub

import java.net.InetSocketAddress

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
class SubscriberClient(prefix          : String,
                       streamName      : String,
                       usedPartitions  : List[Int],
                       zkHosts         : List[InetSocketAddress],
                       zkSessionTimeout    : Int,
                       zkConnectionTimeout : Int) {

  private val zkService   = new ZkService(prefix, zkHosts, zkSessionTimeout, zkConnectionTimeout)
  private val broadcaster = new Broadcaster

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
      watcher.process(null)
    }
  }

  /**
   * Publish [[ProducerTopicMessage]]] to all accepted subscribers
   * @param msg [[ProducerTopicMessage]]]
   */
  def publish(msg : ProducerTopicMessage, onComplete: () => Unit) = {
    broadcaster.broadcast(msg, onComplete)
  }

  /**
   * Update subscribers on specific partition
   */
  private def updateSubscribers(partition : Int) = {
    val subscribersPathOpt = zkService.getAllSubNodesData[String](s"/subscribers/agents/$streamName/$partition")
    if (subscribersPathOpt.isDefined)
      broadcaster.updateSubscribers(subscribersPathOpt.get)
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
   * Stop this Subscriber client
   */
  def stop() = {
    broadcaster.close()
    zkService.close()
  }
}
