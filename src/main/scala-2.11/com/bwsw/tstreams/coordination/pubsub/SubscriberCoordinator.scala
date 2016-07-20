package com.bwsw.tstreams.coordination.pubsub

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import com.bwsw.tstreams.common.zkservice.ZkService
import com.bwsw.tstreams.coordination.pubsub.messages.ProducerTopicMessage
import com.bwsw.tstreams.coordination.pubsub.listener.SubscriberListener
import org.apache.zookeeper.{CreateMode, KeeperException}
import org.slf4j.LoggerFactory


/**
 * @param agentAddress Subscriber address
 * @param zkRootPrefix Zookeeper root prefix for all metadata
 * @param zkHosts Zookeeper hosts to connect
 * @param zkSessionTimeout Zookeeper connect timeout
 */
class SubscriberCoordinator(agentAddress : String,
                          zkRootPrefix : String,
                          zkHosts : List[InetSocketAddress],
                          zkSessionTimeout : Int,
                          zkConnectionTimeout : Int)(implicit system : ActorSystem) {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val SYNCHRONIZE_LIMIT = 60
  private val zkService = new ZkService(zkRootPrefix, zkHosts, zkSessionTimeout, zkConnectionTimeout)
  private val (_,port) = getHostPort(agentAddress)
  private val listener: SubscriberListener = new SubscriberListener(port)
  private var isFinished = false
  private val partitionToUniqueAgentsAmount = scala.collection.mutable.Map[Int, Int]()

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
 *
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
    isFinished = true
  }

  /**
   * @return Coordinator state
   */
  def isStoped = isFinished

  /**
   * Start listen of all [[com.bwsw.tstreams.agents.producer.BasicProducer]]] updates
   */
  def startListen() = {
    listener.start()
  }

  /**
    * Try remove this subscriber if it was already created
    * @param streamName
    * @param partition
    */
  private def tryClean(streamName : String, partition : Int) : Unit = {
    val agentsOpt = zkService.getAllSubPath(s"/subscribers/agents/$streamName/$partition")
    if (agentsOpt.isEmpty)
      return
    val agents: List[String] = agentsOpt.get
    val filtered = agents.filter(_ contains s"_${agentAddress}_")
    filtered foreach { path =>
      try {
        zkService.delete(s"/subscribers/agents/$streamName/$partition/" + path)
      } catch {
        case e : KeeperException =>
      }
    }
  }

  /**
    * Register subscriber
    * on stream/partition
    */
  def registerSubscriber(streamName : String, partition : Int) : Unit = {
    tryClean(streamName, partition)
    zkService.create(s"/subscribers/agents/$streamName/$partition/subscriber_${agentAddress}_", agentAddress, CreateMode.EPHEMERAL_SEQUENTIAL)
  }

  /**
   * Notify all [[com.bwsw.tstreams.agents.producer.BasicProducer]]]
   * about new subscriber
   * on stream/partition
   */
  def notifyProducers(streamName : String, partition : Int) : Unit = {
    listener.resetConnectionsAmount()
    zkService.notify(s"/subscribers/event/$streamName/$partition")
  }

  /**
   * Calculate amount of unique agents on every partition
   * (if agent was on previous partitions it will not be counted)
   */
  def initSynchronization(streamName : String, partitions : List[Int]) : Unit = {
    partitionToUniqueAgentsAmount.clear()
    var alreadyExist = Set[String]()
    partitions foreach { p=>
      val agents = zkService.getAllSubPath(s"/producers/agents/$streamName/$p").getOrElse(List())
      assert(agents.distinct.size == agents.size)
      val filtered = agents.filter(x=> !alreadyExist.contains(x))
      filtered foreach (x=> alreadyExist += x)
      partitionToUniqueAgentsAmount(p) = filtered.size
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
    val amount = partitionToUniqueAgentsAmount(partition)

    logger.debug(s"[SUBSCRIBER COORDINATOR BEFORE SYNC] stream={$streamName} partition={$partition}" +
      s" listener.connectionAmount={${listener.getConnectionsAmount()}} totalConnAmount={$amount}" +
      s" timerVal={$timer}")

    //TODO Сhecks in zk
    while (listener.getConnectionsAmount < amount && timer < SYNCHRONIZE_LIMIT){
      timer += 1
      Thread.sleep(1000)
    }

    logger.debug(s"[SUBSCRIBER COORDINATOR AFTER SYNC] stream={$streamName} partition={$partition}" +
      s" connectionAmount={${listener.getConnectionsAmount()}} totalConnAmount={$amount}" +
      s" timerVal={$timer}")
  }

  /**
   * Global distributed Lock on stream
 *
   * @return [[com.twitter.common.zookeeper.DistributedLockImpl]]]
   */
  def getStreamLock(streamName : String)  = {
    val lock = zkService.getLock(s"/global/stream/$streamName")
    lock
  }
}