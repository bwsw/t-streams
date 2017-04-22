package com.bwsw.tstreams.agents.producer

import java.net.{DatagramPacket, SocketAddress}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import com.bwsw.tstreams.coordination.server.RequestsServer
import com.bwsw.tstreams.proto.protocol.{TransactionRequest, TransactionResponse}
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.leader.LeaderLatch
import org.apache.zookeeper.KeeperException
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.Random

/**
  *
  */
object TransactionOpenerService {

  /**
    * How long to sleep during master elections actions
    */
  val RETRY_SLEEP_TIME = 100
  /**
    * Generic logger object
    */
  val logger = LoggerFactory.getLogger(this.getClass)

}

/**
  * Agent for providing peer to peer interaction between Producers
  *
  * @param curatorClient
  * @param peerKeepAliveTimeout
  * @param producer       Producer reference
  * @param usedPartitions List of used producer partitions
  */
class TransactionOpenerService(curatorClient: CuratorFramework,
                               peerKeepAliveTimeout: Int,
                               producer: Producer,
                               usedPartitions: Set[Int],
                               threadPoolAmount: Int) {


  val myInetAddress = producer.producerOptions.coordinationOptions.openerServerHost
  val myInetPort = producer.producerOptions.coordinationOptions.openerServerPort

  def getSubscriberNotifier() = producer.subscriberNotifier

  private val streamName = producer.stream.name
  private val isRunning = new AtomicBoolean(true)

  /**
    * this ID is used to track sequential transactions from the same master
    */
  private val uniqueAgentId = Math.abs(Random.nextInt() + 1)

  def getAgentAddress() = myInetAddress

  def getProducer() = producer

  def getUniqueAgentID() = uniqueAgentId

  def getAndIncSequentialID(partition: Int): Long = sequentialIds(partition).getAndIncrement()

  /**
    * this ID map is used to track sequential transactions on subscribers
    */
  private val sequentialIds = mutable.Map[Int, AtomicLong]()

  TransactionOpenerService.logger.info(s"[INIT] Start initialize agent with address: {$myInetAddress}")
  TransactionOpenerService.logger.info(s"[INIT] Stream: {$streamName}, partitions: [${usedPartitions.mkString(",")}]")
  TransactionOpenerService.logger.info(s"[INIT] Master Unique random ID: $uniqueAgentId")

  private val localLeaderMap = mutable.Map[Int, (String, Int)]()
  private val leaderMap = mutable.Map[Int, LeaderLatch]()

  val agent = this
  private val openerServer = new RequestsServer(myInetAddress, myInetPort, threadPoolAmount) {

    override def handleRequest(client: SocketAddress, reqAny: AnyRef): Unit = {
      if (!isRunning.get())
        return

      val req = reqAny.asInstanceOf[TransactionRequest]
      val master = agent.isMasterOfPartition(req.partition)

      val newTransactionId = master match {
        case true =>
          val newId = agent.getProducer.generateNewTransactionIDLocal()
          agent.getProducer().openTransactionLocal(newId, req.partition)
          newId
        case _ => 0
      }

      val response = TransactionResponse()
        .withId(req.id)
        .withPartition(req.partition)
        .withTransaction(newTransactionId)
        .toByteArray
      socket.send(new DatagramPacket(response, response.size, client))
    }
  }

  openerServer.start()

  // fill initial sequential counters
  usedPartitions foreach { p => sequentialIds += (p -> new AtomicLong(0)) }

  @tailrec
  private def getLeaderId(partition: Int): String = {
    try {
      leaderMap(partition).getLeader().getId
    } catch {
      case e: KeeperException =>
        Thread.sleep(50)
        getLeaderId(partition)
    }
  }

  private def updatePartitionMasterInetAddress(partition: Int): Unit = leaderMap.synchronized {
    if (!usedPartitions.contains(partition))
      return

    if (!leaderMap.contains(partition)) {
      try {
        curatorClient.create.forPath(s"/master-$partition")
      } catch {
        case e: KeeperException =>
          if (e.code() != KeeperException.Code.NODEEXISTS)
            throw e
      }

      val leader = new LeaderLatch(curatorClient, s"/master-$partition", s"${myInetAddress}:${myInetPort}#$uniqueAgentId")
      leaderMap(partition) = leader
      leader.start()
    }

    var leaderInfo = getLeaderId(partition)
    while (leaderInfo == "") {
      leaderInfo = getLeaderId(partition)
      Thread.sleep(50)
    }
    val parts = leaderInfo.split('#')
    val leaderAddress = parts.head
    val leaderId = Integer.parseInt(parts.tail.head)

    localLeaderMap.synchronized {
      localLeaderMap(partition) = (leaderAddress, leaderId)
    }
  }


  def getPartitionMasterInetAddressLocal(partition: Int): (String, Int) = localLeaderMap.synchronized {
    if (!localLeaderMap.contains(partition)) {
      updatePartitionMasterInetAddress(partition)
    }
    localLeaderMap(partition)
  }

  def isMasterOfPartition(partition: Int): Boolean = leaderMap.synchronized {
    if (!usedPartitions.contains(partition))
      return false

    if (!leaderMap.contains(partition)) {
      updatePartitionMasterInetAddress(partition)
    }
    leaderMap(partition).hasLeadership()
  }


  /**
    * Retrieve new transaction from agent
    *
    * @param partition Transaction partition
    * @return TransactionID
    */
  def generateNewTransaction(partition: Int): Long = this.synchronized {
    val master = getPartitionMasterInetAddressLocal(partition)._1
    if (TransactionOpenerService.logger.isDebugEnabled) {
      TransactionOpenerService.logger.debug(s"[GET TRANSACTION] Start retrieve transaction for agent with address: {$myInetAddress}, stream: {$streamName}, partition: {$partition} from [MASTER: {$master}].")
    }

    val res =
      producer.transactionRequest(master, partition) match {
        case None =>
          updatePartitionMasterInetAddress(partition)
          generateNewTransaction(partition)

        case Some(TransactionResponse(_, p, transactionID)) if transactionID <= 0 =>
          updatePartitionMasterInetAddress(partition)
          generateNewTransaction(partition)

        case Some(TransactionResponse(_, p, transactionID)) if transactionID > 0 =>
          if (TransactionOpenerService.logger.isDebugEnabled) {
            TransactionOpenerService.logger.debug(s"[GET TRANSACTION] Finish retrieve transaction for agent with address: $myInetAddress, stream: $streamName, partition: $partition with ID: $transactionID from [MASTER: $master]s")
          }
          transactionID
      }
    res
  }


  /**
    * Stop this agent
    */
  def stop() = {
    TransactionOpenerService.logger.info(s"P2PAgent of ${producer.name} is shutting down.")
    isRunning.set(false)
    leaderMap.foreach(leader => leader._2.close())
    openerServer.stop()
  }

}