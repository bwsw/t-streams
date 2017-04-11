package com.bwsw.tstreams.coordination.client

import java.net.{DatagramSocket, InetAddress}
import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreams.common.ProtocolMessageSerializer
import com.bwsw.tstreams.coordination.messages.state.TransactionStateMessage
import org.apache.curator.framework.CuratorFramework
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._


object UdpEventsBroadcastClient {
  val logger = LoggerFactory.getLogger(this.getClass)
}

/**
  *
  * @param curatorClient
  * @param partitions
  */
class UdpEventsBroadcastClient(curatorClient: CuratorFramework, partitions: Set[Int]) {

  private val UPDATE_PERIOD_MS = 1000

  private val isStopped = new AtomicBoolean(true)

  private val clientSocket = new DatagramSocket()

  private val partitionSubscribers = new java.util.concurrent.ConcurrentHashMap[Int, Set[String]]()

  private val updateThread = new Thread(() => {
    while (!isStopped.get()) {
      Thread.sleep(UPDATE_PERIOD_MS)
      partitions.foreach(p => updateSubscribers(p))
    }
  })

  /**
    * Initialize coordinator
    */
  def init(): Unit = {
    isStopped.set(false)
    partitions.foreach { p => {
      partitionSubscribers.put(p, Set[String]().empty)
      updateSubscribers(p)
    }
    }
    updateThread.start()
  }

  private def broadcast(set: Set[String], msg: TransactionStateMessage) = {
    set.foreach(address => {
      val splits = address.split(":")
      //assert(splits.size == 2)
      val host = InetAddress.getByName(splits(0))
      val port = splits(1).toInt
      val msgString = ProtocolMessageSerializer.serialize(msg)
      val bytes = msgString.getBytes()
      val sendPacket = new java.net.DatagramPacket(bytes, bytes.length, host, port)
      try {
        clientSocket.send(sendPacket)
      } catch {
        case e: Exception =>
          UdpEventsBroadcastClient.logger.warn(s"Send $msgString to $host:$port failed. Exception is: $e")
      }
    })
  }

  /**
    * Publish Message to all accepted subscribers
    *
    * @param msg Message
    */
  def publish(msg: TransactionStateMessage): Unit = {
    if (!isStopped.get) {
      val set = partitionSubscribers.get(msg.partition)
      broadcast(set, msg)
    }
  }

  /**
    * Update subscribers on specific partition
    */
  private def updateSubscribers(partition: Int) = {
    if (curatorClient.checkExists.forPath(s"/subscribers/$partition") != null) {
      val newPeers = curatorClient.getChildren.forPath(s"/subscribers/$partition").asScala.toSet
      partitionSubscribers.put(partition, newPeers)
    }
  }

  /**
    * Stop this Subscriber client
    */
  def stop() = {
    if (isStopped.getAndSet(true))
      throw new IllegalStateException("Producer->Subscriber notifier was stopped second time.")

    updateThread.join()
    clientSocket.close()
    partitionSubscribers.clear()
  }
}
