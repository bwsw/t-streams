package com.bwsw.tstreams.coordination.pubsub.publisher

import java.net.InetSocketAddress
import java.util.concurrent.locks.ReentrantLock

import io.netty.bootstrap.Bootstrap
import io.netty.channel.ChannelId
import org.slf4j.LoggerFactory

class BroadcasterConnectionManager(bootstrap: Bootstrap) {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val idToAddress = scala.collection.mutable.Map[ChannelId, String]()
  private val addressToId = scala.collection.mutable.Map[String, ChannelId]()
  private val lock = new ReentrantLock(true)

  def updateSubscribers(newSubscribers: List[String]) = {
    lock.lock()
    logger.debug(s"[BROADCASTER] start updating subscribers:{${addressToId.keys.mkString(",")}}" +
      s" using newSubscribers:{${newSubscribers.mkString(",")}}")
    newSubscribers.diff(addressToId.keys.toList) foreach { subscriber =>
      this.connect(subscriber)
    }
    logger.debug(s"[BROADCASTER] updated subscribers:{${addressToId.keys.mkString(",")}}")
    lock.unlock()
  }

  def channelInactive(id: ChannelId) = {
    lock.lock()
    if (idToAddress.contains(id)) {
      val address = idToAddress(id)
      idToAddress.remove(id)
      addressToId.remove(address)
    }
    lock.unlock()
  }

  private def connect(subscriber: String) = {
    val splits = subscriber.split(":")
    assert(splits.size == 2)
    val host = splits(0)
    val port = splits(1).toInt
    val channelFuture = bootstrap.connect(new InetSocketAddress(host, port)).sync()
    if (channelFuture.isSuccess) {
      idToAddress(channelFuture.channel().id()) = subscriber
      addressToId(subscriber) = channelFuture.channel().id()
    }
  }
}
