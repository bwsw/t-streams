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
  * @param prefix
  * @param producer       Producer reference
  * @param usedPartitions List of used producer partitions
  */
class TransactionOpenerService(curatorClient: CuratorFramework,
                               prefix: String,
                               producer: Producer,
                               usedPartitions: Set[Int],
                               threadPoolAmount: Int) {


  val myInetAddress = producer.producerOptions.coordinationOptions.openerServerHost
  val myInetPort = producer.producerOptions.coordinationOptions.openerServerPort

  private val streamName = producer.stream.name
  private val isRunning = new AtomicBoolean(true)

  /**
    * this ID is used to track sequential transactions from the same master
    */
  private val uniqueAgentId = Math.abs(Random.nextInt() + 1)

  def getAgentAddress = myInetAddress

  def getProducer = producer

  def getUniqueAgentID = uniqueAgentId

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
      if (!isRunning.get()) {
        if (Producer.logger.isDebugEnabled())
          Producer.logger.debug(s"[handleRequest $uniqueAgentId] Agent with address: {$myInetAddress}, stream: {$streamName} is not running")
        return
      }

      val req = reqAny.asInstanceOf[TransactionRequest]
      if(req.authKey != producer.stream.client.authenticationKey) {
        Producer.logger.warn(s"Authentication Key ${req.authKey} received, but expected is ${producer.stream.client.authenticationKey}, message has been ignored.")
        return
      }

      val isMaster = agent.isMasterOfPartition(req.partition)
      val newTransactionId = if (isMaster) {
        val newId = agent.getProducer.generateNewTransactionIDLocal()

        if (Producer.logger.isDebugEnabled())
          Producer.logger.debug(s"[$uniqueAgentId] New Transaction ID: $newId (partition ${req.partition})")

        (req.isInstant, req.isReliable) match {
          case (false, _) => agent.getProducer.openTransactionLocal(newId, req.partition)
          case (true, _) => agent.getProducer.openInstantTransactionLocal(req.partition, newId, req.data.map(_.toByteArray), req.isReliable)
        }

        newId
      } else {
        0
      }

      val response = TransactionResponse()
        .withId(req.id)
        .withPartition(req.partition)
        .withTransaction(newTransactionId)
        .withAuthKey(req.authKey)
        .toByteArray
      socket.send(new DatagramPacket(response, response.size, client))

      if (Producer.logger.isDebugEnabled())
        Producer.logger.debug(s"Send a response from master to client that a new transaction: $newTransactionId has been opened")
    }
  }

  openerServer.start()

  // fill initial sequential counters
  usedPartitions foreach { p => sequentialIds += (p -> new AtomicLong(0)) }

  @tailrec
  private def getLeaderId(partition: Int): String = {
    try {
      if (Producer.logger.isDebugEnabled())
        Producer.logger.debug(s"[$uniqueAgentId] Try getting a leader id for partition: $partition")
      val leader = leaderMap(partition).getLeader()
      if (Producer.logger.isDebugEnabled())
        Producer.logger.debug(s"[$uniqueAgentId] Get a leader for partition: $partition: ${leader.isLeader} (is leader)")

      leader.getId
    } catch {
      case e: KeeperException =>
        if (Producer.logger.isDebugEnabled())
          Producer.logger.debug(s"[$uniqueAgentId] Catch the exception while getting a leader id for partition: $partition. Exception: $e")
        Thread.sleep(50)
        getLeaderId(partition)
    }
  }

  private def updatePartitionMasterInetAddress(partition: Int): Unit = this.synchronized {
    if (Producer.logger.isDebugEnabled)
      Producer.logger.debug(s"[PARTITION_$partition $uniqueAgentId] update a partition master address")
    if (!usedPartitions.contains(partition)) {
      if (Producer.logger.isDebugEnabled)
        Producer.logger.debug(s"[PARTITION_$partition $uniqueAgentId] is not included to range of used partitions (${usedPartitions.mkString(",")})")
      return
    }

    if (!leaderMap.contains(partition)) {
      try {
        curatorClient.create.forPath(s"$prefix/master-$partition")
      } catch {
        case e: KeeperException =>
          if (e.code() != KeeperException.Code.NODEEXISTS)
            throw e
      }

      val leader = new LeaderLatch(curatorClient, s"$prefix/master-$partition", s"$myInetAddress:$myInetPort#$uniqueAgentId")
      if (Producer.logger.isDebugEnabled)
        Producer.logger.debug(s"[PARTITION_$partition $uniqueAgentId] Create a new leader latch with ID: $myInetAddress:$myInetPort#$uniqueAgentId")
      leaderMap(partition) = leader
      if (Producer.logger.isDebugEnabled)
        Producer.logger.debug(s"[PARTITION_$partition $uniqueAgentId] Start the leader latch")
      leader.start()
      if (Producer.logger.isDebugEnabled)
        Producer.logger.debug(s"[PARTITION_$partition $uniqueAgentId] Set the leader for partition: $myInetAddress:$myInetPort#$uniqueAgentId (is leader: ${leader.hasLeadership})")
    }

    var leaderInfo = getLeaderId(partition)
    while (leaderInfo == "") {
      if (Producer.logger.isDebugEnabled())
        Producer.logger.debug(s"[PARTITION_$partition $uniqueAgentId] LeaderInfo has been empty so try to get it again")
      leaderInfo = getLeaderId(partition)
      Thread.sleep(50)
    }
    if (Producer.logger.isDebugEnabled())
      Producer.logger.debug(s"[PARTITION_$partition $uniqueAgentId] Get a leader id: [LEADER_ID=$leaderInfo]")
    val parts = leaderInfo.split('#')
    val leaderAddress = parts.head
    val leaderId = Integer.parseInt(parts.tail.head)

    localLeaderMap(partition) = (leaderAddress, leaderId)
    if (Producer.logger.isDebugEnabled)
      Producer.logger.debug(s"[PARTITION_$partition $uniqueAgentId] Set the leader for partition: ADDRESS=$leaderAddress, ID=$leaderId." +
        s"Actually leader info is ${localLeaderMap(partition)}")

  }


  def getPartitionMasterInetAddressLocal(partition: Int): (String, Int) = this.synchronized {
    if (Producer.logger.isDebugEnabled)
      Producer.logger.debug(s"[PARTITION_$partition $uniqueAgentId] get an address of partition master")
    if (!localLeaderMap.contains(partition)) {
      updatePartitionMasterInetAddress(partition)
    }

    localLeaderMap(partition)
  }

  def isMasterOfPartition(partition: Int): Boolean = this.synchronized {
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
  def generateNewTransaction(partition: Int, isInstant: Boolean = false, isReliable: Boolean = true, data: Seq[Array[Byte]] = Seq()): Long = {
    val master = getPartitionMasterInetAddressLocal(partition)._1
    if (TransactionOpenerService.logger.isDebugEnabled) {
      TransactionOpenerService.logger.debug(s"[GET TRANSACTION $uniqueAgentId] Start retrieve transaction for agent with address: {$myInetAddress:$myInetPort}, stream: {$streamName}, partition: {$partition} from [MASTER: {$master}].")
    }

    val res =
      producer.transactionRequest(master, partition, isInstant, isReliable, data) match {
        case None =>
          if (TransactionOpenerService.logger.isDebugEnabled) {
            TransactionOpenerService.logger.debug(s"[GET TRANSACTION $uniqueAgentId] Get NONE for agent with address: $myInetAddress:$myInetPort, stream: $streamName, partition: $partition from [MASTER: $master]s")
          }
          updatePartitionMasterInetAddress(partition)
          generateNewTransaction(partition)

        case Some(TransactionResponse(_, p, transactionID, masterID, authKey)) if transactionID <= 0 && authKey == producer.stream.client.authenticationKey =>
          if (TransactionOpenerService.logger.isDebugEnabled) {
            TransactionOpenerService.logger.debug(s"[GET TRANSACTION $uniqueAgentId] Get an invalid transaction id ($transactionID) for agent with address: $myInetAddress:$myInetPort, stream: $streamName, partition: $partition from [MASTER: $master]s")
          }
          updatePartitionMasterInetAddress(partition)
          generateNewTransaction(partition)

        case Some(TransactionResponse(_, p, transactionID, masterID, authKey)) if transactionID > 0 && authKey == producer.stream.client.authenticationKey =>
          if (TransactionOpenerService.logger.isDebugEnabled) {
            TransactionOpenerService.logger.debug(s"[GET TRANSACTION $uniqueAgentId] Finish retrieve transaction for agent with address: $myInetAddress:$myInetPort, stream: $streamName, partition: $partition with ID: $transactionID from [MASTER: $master]s")
          }
          transactionID

        case Some(TransactionResponse(_, _, _, _, authKey)) =>
          throw new IllegalStateException(s"Authentication Key ${authKey} received, but expected is ${producer.stream.client.authenticationKey}, message has been ignore.")
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