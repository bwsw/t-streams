package com.bwsw.tstreams.coordination.pubsub

import java.net.InetSocketAddress

import com.bwsw.tstreams.common.zkservice.ZkService
import com.bwsw.tstreams.coordination.pubsub.messages.ProducerTopicMessage
import com.bwsw.tstreams.coordination.pubsub.listener.ProducerTopicMessageListener
import org.apache.zookeeper.CreateMode

import scala.collection.mutable.ListBuffer

/**
 * Consumer coordinator
 * @param agentAddress Consumer address
 * @param zkRootPrefix Zookeeper root prefix for all metadata
 * @param zkHosts Zookeeper hosts to connect
 * @param zkSessionTimeout Zookeeper connect timeout
 */
class ConsumerCoordinator(agentAddress : String,
                          zkRootPrefix : String,
                          zkHosts : List[InetSocketAddress],
                          zkSessionTimeout : Int) {
  private val SYNCHRONIZE_LIMIT = 60
  private val zkService = new ZkService(zkRootPrefix, zkHosts, zkSessionTimeout)
  private val (_,port) = getHostPort(agentAddress)
  private val listener: ProducerTopicMessageListener = new ProducerTopicMessageListener(port)

  private def getHostPort(address : String): (String, Int) = {
    val splits = address.split(":")
    assert(splits.size == 2)
    val host = splits(0)
    val port = splits(1).toInt
    (host, port)
  }

  def addCallback(callback : (ProducerTopicMessage) => Unit) = {
    listener.addCallbackToChannelHandler(callback)
  }

  def stop() = {
    listener.stop()
    zkService.close()
  }

  def startListen() = {
    listener.start()
  }

  def startCallback() = {
    listener.startCallback()
  }

  def stopCallback() = {
    listener.stopCallback()
  }

  def registerSubscriber(streamName : String, partition : Int) = {
    zkService.create(s"/subscribers/agents/$streamName/$partition/subscriber_", agentAddress, CreateMode.EPHEMERAL_SEQUENTIAL)
  }

  def notifyProducers(streamName : String, partition : Int) = {
    zkService.notify(s"/subscribers/event/$streamName/$partition")
  }

  def synchronize(streamName : String, partitions : List[Int]) = {
    val buf = ListBuffer[String]()
    partitions foreach { p =>
      buf.append(zkService.getAllSubPath(s"/producers/agents/$streamName/$p").getOrElse(List()):_*)
    }
    val totalAmount = buf.distinct.size
    var timer = 0
    while (listener.getConnectionsAmount < totalAmount && timer < SYNCHRONIZE_LIMIT){
      timer += 1
      Thread.sleep(1000)
    }
  }

  def getStreamLock(streamName : String)  = {
    val lock = zkService.getLock(s"/global/stream/$streamName")
    lock
  }
}