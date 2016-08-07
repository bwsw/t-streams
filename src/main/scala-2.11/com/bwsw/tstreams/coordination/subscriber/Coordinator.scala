package com.bwsw.tstreams.coordination.subscriber

import java.net.InetSocketAddress
import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreams.common.ZookeeperDLMService
import com.bwsw.tstreams.coordination.messages.state.Message
import org.apache.zookeeper.{CreateMode, KeeperException}
import org.slf4j.LoggerFactory


/**
  * @param agentAddress     Subscriber address
  * @param zkRootPrefix     Zookeeper root prefix for all metadata
  * @param zkHosts          Zookeeper hosts to connect
  * @param zkSessionTimeout Zookeeper connect timeout
  */
class Coordinator(agentAddress: String,
                  zkRootPrefix: String,
                  zkHosts: List[InetSocketAddress],
                  zkSessionTimeout: Int,
                  zkConnectionTimeout: Int) {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val SYNCHRONIZE_LIMIT = 60
  private val zkService = new ZookeeperDLMService(zkRootPrefix, zkHosts, zkSessionTimeout, zkConnectionTimeout)
  private val (host, port) = getHostPort(agentAddress)
  private val listener: ProducerEventReceiverTcpServer = new ProducerEventReceiverTcpServer(host, port)
  private val stopped = new AtomicBoolean(false)
  private val partitionToUniqueAgentsAmount = scala.collection.mutable.Map[Int, Int]()

  /**
    * Extract host/port from string
    */
  private def getHostPort(address: String): (String, Int) = {
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
  def addCallback(callback: (Message) => Unit) = {
    if(stopped.get)
      throw new IllegalStateException("Subscriber coordinator is closed already.")

    listener.addCallbackToChannelHandler(callback)
  }

  /**
    * Stop this coordinator
    */
  def stop() = {
    if(stopped.getAndSet(true))
      throw new IllegalStateException("Subscriber coordinator is closed already. Double close situation detected.")
    listener.stop()
    zkService.close()
  }

  /**
    * @return Coordinator state
    */
  def isStoped: Boolean = stopped.get()

  /**
    * Start listen of all [[com.bwsw.tstreams.agents.producer.Producer]]] updates
    */
  def startListen() = {
    if(stopped.get)
      throw new IllegalStateException("Subscriber coordinator is closed already.")

    listener.start()
  }

  /**
    * Try remove this subscriber if it was already created
    *
    * @param streamName
    * @param partition
    */
  private def tryClean(streamName: String, partition: Int): Unit = {
    if(stopped.get)
      throw new IllegalStateException("Subscriber coordinator is closed already.")

    val agentsOpt = zkService.getAllSubPath(s"/subscribers/agents/$streamName/$partition")
    if (agentsOpt.isEmpty)
      return
    val agents: List[String] = agentsOpt.get
    val filtered = agents.filter(_ contains s"_${agentAddress}_")
    filtered foreach { path =>
      try {
        zkService.delete(s"/subscribers/agents/$streamName/$partition/" + path)
      } catch {
        case e: KeeperException =>
      }
    }
  }

  /**
    * Register subscriber
    * on stream/partition
    */
  def registerSubscriber(streamName: String, partition: Int): Unit = {
    if(stopped.get)
      throw new IllegalStateException("Subscriber coordinator is closed already.")

    tryClean(streamName, partition)
    zkService.create(s"/subscribers/agents/$streamName/$partition/subscriber_${agentAddress}_", agentAddress, CreateMode.EPHEMERAL_SEQUENTIAL)
  }

  /**
    * Notify all [[com.bwsw.tstreams.agents.producer.Producer]]]
    * about new subscriber
    * on stream/partition
    */
  def notifyProducers(streamName: String, partition: Int): Unit = {
    if(stopped.get)
      throw new IllegalStateException("Subscriber coordinator is closed already.")

    listener.resetConnectionsAmount()
    zkService.notify(s"/subscribers/event/$streamName/$partition")
  }

  /**
    * Calculate amount of unique agents on every partition
    * (if agent was on previous partitions it will not be counted)
    */
  def initSynchronization(streamName: String, partitions: List[Int]): Unit = {
    if(stopped.get)
      throw new IllegalStateException("Subscriber coordinator is closed already.")

    partitionToUniqueAgentsAmount.clear()
    var alreadyExist = Set[String]()
    partitions foreach { p =>
      val agents = zkService.getAllSubPath(s"/producers/agents/$streamName/$p").getOrElse(List())
      assert(agents.distinct.size == agents.size)
      val filtered = agents.filter(x => !alreadyExist.contains(x))
      filtered foreach (x => alreadyExist += x)
      partitionToUniqueAgentsAmount(p) = filtered.size
    }
  }

  /**
    * Synchronize subscriber with all [[com.bwsw.tstreams.agents.producer.Producer]]]
    * just wait when all producers will connect to subscriber
    * because of stream lock it is continuous number
    * on stream/partition
    */
  def synchronize(streamName: String, partition: Int) = {
    if(stopped.get)
      throw new IllegalStateException("Subscriber coordinator is closed already.")

    var timer = 0
    val amount = partitionToUniqueAgentsAmount(partition)

    logger.debug(s"[SUBSCRIBER COORDINATOR BEFORE SYNC] stream={$streamName} partition={$partition}" +
      s" listener.connectionAmount={${listener.getConnectionsAmount()}} totalConnAmount={$amount}" +
      s" timerVal={$timer}")

    //TODO Сhecks in zk
    while (listener.getConnectionsAmount < amount && timer < SYNCHRONIZE_LIMIT) {
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
  def getStreamLock(streamName: String) = {
    if(stopped.get)
      throw new IllegalStateException("Subscriber coordinator is closed already.")

    val lock = zkService.getLock(s"/global/stream/$streamName")
    lock
  }
}