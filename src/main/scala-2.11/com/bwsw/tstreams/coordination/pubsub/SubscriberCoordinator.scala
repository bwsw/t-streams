package com.bwsw.tstreams.coordination.pubsub

import java.net.InetSocketAddress

import com.bwsw.tstreams.common.zkservice.ZkService
import com.bwsw.tstreams.coordination.pubsub.messages.ProducerTopicMessage
import com.bwsw.tstreams.coordination.pubsub.listener.ProducerTopicMessageListener
import org.apache.zookeeper.CreateMode
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

/**
 * @param agentAddress Subscriber address
 * @param zkRootPrefix Zookeeper root prefix for all metadata
 * @param zkHosts Zookeeper hosts to connect
 * @param zkSessionTimeout Zookeeper connect timeout
 */
class SubscriberCoordinator(agentAddress : String,
                          zkRootPrefix : String,
                          zkHosts : List[InetSocketAddress],
                          zkSessionTimeout : Int) {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val SYNCHRONIZE_LIMIT = 60
  private val zkService = new ZkService(zkRootPrefix, zkHosts, zkSessionTimeout)
  private val (_,port) = getHostPort(agentAddress)
  private val listener: ProducerTopicMessageListener = new ProducerTopicMessageListener(port)
  private var stoped = false
  private val partitionToUniqAgentsAmount = scala.collection.mutable.Map[Int, Int]()

  /**
   * Extract host/port from string
   */
  private def getHostPort(address : String): (String, Int) = {
    val splits = address.split(":")
    assert(splits.size == 2)
    val host = splits(0)
    val port = splits(1).toInt
    (host, port)
  }

  /**
   * Add new event callback to listener
   * @param callback Event callback
   */
  def addCallback(callback : (ProducerTopicMessage) => Unit) = {
    listener.addCallbackToChannelHandler(callback)
  }

  /**
   * Stop this coordinator
   */
  def stop() = {
    listener.stop()
    zkService.close()
    stoped = true
  }

  /**
   * @return Coordinator state
   */
  def isStoped = stoped

  /**
   * Start listen of all [[com.bwsw.tstreams.agents.producer.BasicProducer]]] updates
   */
  def startListen() = {
    listener.start()
  }

  /**
   * Start callback on incoming updates of
   * [[com.bwsw.tstreams.agents.producer.BasicProducer]]]
   */
  def startCallback() = {
    listener.startCallback()
  }

  /**
   * Register subscriber
   * on stream/partition
   */
  def registerSubscriber(streamName : String, partition : Int) : Unit = {
    zkService.create(s"/subscribers/agents/$streamName/$partition/subscriber_", agentAddress, CreateMode.EPHEMERAL_SEQUENTIAL)
  }

  /**
   * Notify all [[com.bwsw.tstreams.agents.producer.BasicProducer]]]
   * about new subscriber
   * on stream/partition
   */
  def notifyProducers(streamName : String, partition : Int) : Unit = {
    zkService.notify(s"/subscribers/event/$streamName/$partition")
  }

  /**
   * Calculate amount of unique agents on every partition
   * (if agent was on previous partitions it will not be counted)
   */
  def initSynchronization(streamName : String, partitions : List[Int]) : Unit = {
    partitionToUniqAgentsAmount.clear()
    var alreadyExist = Set[String]()
    partitions foreach { p=>
      val agents = zkService.getAllSubPath(s"/producers/agents/$streamName/$p").getOrElse(List())
      assert(agents.distinct.size == agents.size)
      val filtered = agents.filter(x=> !alreadyExist.contains(x))
      filtered foreach (x=> alreadyExist += x)
      partitionToUniqAgentsAmount(p) = filtered.size
    }
  }

  /**
   * Synchronize subscriber with all [[com.bwsw.tstreams.agents.producer.BasicProducer]]]
   * just wait when all producers will connect to subscriber
   * because of stream lock it is continuous number
   * on stream/partition
   */
  def synchronize(streamName : String, partition : Int) = {
    var timer = 0
    listener.resetConnectionsAmount()
    val amount = partitionToUniqAgentsAmount(partition)
    while (listener.getConnectionsAmount < amount && timer < SYNCHRONIZE_LIMIT){
      timer += 1
      Thread.sleep(1000)
    }

    logger.debug(s"[SUBSCRIBER COORDINATOR] stream={$streamName} partition={$partition}" +
      s" listener.connectionAmount={${listener.getConnectionsAmount()}} totalConnAmount={$amount}" +
      s" timerVal={$timer}")
  }

  /**
   * Global distributed Lock on stream
   * @return [[com.twitter.common.zookeeper.DistributedLockImpl]]]
   */
  def getStreamLock(streamName : String)  = {
    val lock = zkService.getLock(s"/global/stream/$streamName")
    lock
  }
}