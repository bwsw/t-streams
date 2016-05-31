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
                          zkSessionTimeout : Int) {
  private val zkService = new ZkService(prefix, zkHosts, zkSessionTimeout)
  private val broadcaster = new Broadcaster
  private val lock = new ReentrantLock(true)

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

  def publish(msg : ProducerTopicMessage) = {
    lock.lock()
    broadcaster.broadcast(msg)
    lock.unlock()
  }

  private def updateSubscribers(partition : Int) = {
    lock.lock()
    val subscribersPathOpt = zkService.getAllSubNodesData[String](s"/subscribers/agents/$streamName/$partition")
    if (subscribersPathOpt.isDefined)
      broadcaster.updateSubscribers(subscribersPathOpt.get)
    lock.unlock()
  }

  def getStreamLock(streamName : String)  = {
    val lock = zkService.getLock(s"/global/stream/$streamName")
    lock
  }

  def stop() = {
    broadcaster.close()
    zkService.close()
  }
}
