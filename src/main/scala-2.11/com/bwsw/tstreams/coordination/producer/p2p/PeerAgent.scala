package com.bwsw.tstreams.coordination.producer.p2p

import java.net.InetSocketAddress
import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.producer.Producer
import com.bwsw.tstreams.common.{FirstFailLockableTaskExecutor, LockUtil, ZookeeperDLMService}
import com.bwsw.tstreams.coordination.messages.master._
import com.bwsw.tstreams.coordination.messages.state.{Message, TransactionStatus}
import com.bwsw.tstreams.coordination.producer.transport.traits.ITransport
import org.apache.zookeeper.{CreateMode, KeeperException}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.Random


/**
 * Agent for providing peer to peer interaction between [[Producer]]]
 *
 * @param agentAddress Concrete agent address
 * @param zkHosts ZkHosts to connect
 * @param zkRootPath Common prefix for all zk created entities
 * @param zkSessionTimeout Session Zk Timeout
 * @param producer Producer reference
 * @param usedPartitions List of used producer partitions
 * @param isLowPriorityToBeMaster Flag which indicate to have low priority to be master
 * @param transport Transport to provide interaction
 * @param transportTimeout Timeout for waiting response
 */
class PeerAgent(agentAddress : String,
                zkHosts : List[InetSocketAddress],
                zkRootPath : String,
                zkSessionTimeout: Int,
                zkConnectionTimeout : Int,
                producer : Producer[_],
                usedPartitions : List[Int],
                isLowPriorityToBeMaster : Boolean,
                transport: ITransport,
                transportTimeout : Int,
                poolSize : Int) {

  private val zkRetriesAmount = 60
  private val externalAccessLock = new ReentrantLock(true)
  private val zkService = new ZookeeperDLMService(zkRootPath, zkHosts, zkSessionTimeout, zkConnectionTimeout)

  val localMasters = new java.util.concurrent.ConcurrentHashMap[Int /*partition*/ , String /*master address*/ ]()
  val logger = LoggerFactory.getLogger(this.getClass)

  private val lockManagingMaster = new ReentrantLock(true)
  private val streamName = producer.stream.getName
  private val isRunning = new AtomicBoolean(true)
  private var zkConnectionValidator: Thread = null
  private var messageHandler: Thread = null

  def getAgentAddress = agentAddress

  def getTransport = transport

  def getUsedPartitions = usedPartitions

  def getProducer = producer

  /**
    * this ID is used to track sequential transactions from the same master
    */
  private val uniqueAgentId = Random.nextInt()

  /**
    * this ID map is used to track sequential transactions on subscribers
    */
  private val sequentialIds = mutable.Map[Int, AtomicLong]()

  /**
    * For making low priority masters
    *
    */
  private val LOW_PRIORITY_PENALTY = 1000 * 1000

  logger.info(s"[INIT] Start initialize agent with address: {$agentAddress}")
  logger.info(s"[INIT] Stream: {$streamName}, partitions: [${usedPartitions.mkString(",")}]")
  logger.info(s"[INIT] Master Unique random ID: $uniqueAgentId")

  usedPartitions foreach { p => tryInvalidateThisPeer(p) }

  transport.bindLocalAddress(agentAddress)

  startSessionKeepAliveThread()
  beginHandleMessages()

  usedPartitions foreach { p =>
    // fill initial sequential counters
    sequentialIds += (p -> new AtomicLong(0))
    // save initial records to zk
    val penalty = if (isLowPriorityToBeMaster) LOW_PRIORITY_PENALTY else 0

    zkService.create[AgentSettings](s"/producers/agents/$streamName/$p/agent_${agentAddress}_$uniqueAgentId",
      AgentSettings(agentAddress, priority = 0, penalty),
      CreateMode.EPHEMERAL_SEQUENTIAL)
  }

  usedPartitions foreach { p =>
    updateMaster(p, init = true)
  }

  logger.debug(s"[INIT] Finish initialize agent with address:{$agentAddress}," +
    s"stream:{$streamName},partitions:{${usedPartitions.mkString(",")}\n")

  /**
    * Invalidate this peer overdue information in case if it exist
    *
    * @param partition Partition to check
    */
  private def tryInvalidateThisPeer(partition: Int): Unit = {
    val agentsOpt = zkService.getAllSubPath(s"/producers/agents/$streamName/$partition")
    agentsOpt.foreach{ agents =>
      //try to remove overdue agents information
      val filtered = agents.filter(x => x.contains(agentAddress) && !x.contains(uniqueAgentId))
      filtered foreach { path =>
        logger.debug(s"[INIT CLEAN] Delete agent on address:{$path} on" +
          s"stream:{$streamName},partition:{$partition}\n")
        try {
          zkService.delete(s"/producers/agents/$streamName/$partition/" + path)
        } catch {
          case e: KeeperException =>
        }

      }
    }
    //try to remove old master
    val master = getMaster(partition)
    master.foreach{masterSettings =>
      if (masterSettings.agentAddress == agentAddress &&
          masterSettings.uniqueAgentId != uniqueAgentId){
        logger.debug(s"[INIT CLEAN] Delete agent as MASTER on address:{$agentAddress} on" +
          s"stream:{$streamName},partition:{$partition} because id was overdue\n")
        deleteThisAgentFromMasters(partition)
      }
    }

    logger.debug(s"[INIT CLEAN FINISHED] Delete agent on address:{$agentAddress} on" +
      s"stream:{$streamName},partition:{$partition}\n")
  }

  /**
    * Amend agent priority
    *
    * @param partition Partition to update priority
    * @param value     Value which will be added to current priority
    */
  def updateThisAgentPriority(partition: Int, value: Int) = {
    logger.debug(s"[PRIOR] Start amend agent priority with value:{$value} with address:{$agentAddress}" +
      s" on stream:{$streamName},partition:{$partition}\n")
    val agentsOpt = zkService.getAllSubPath(s"/producers/agents/$streamName/$partition")
    assert(agentsOpt.isDefined)
    val agents: List[String] = agentsOpt.get
    val filtered = agents.filter(_ contains agentAddress)
    assert(filtered.size == 1)
    val thisAgentPath = filtered.head
    val agentSettingsOpt = zkService.get[AgentSettings](s"/producers/agents/$streamName/$partition/" + thisAgentPath)
    assert(agentSettingsOpt.isDefined)
    val updatedAgentSettings = agentSettingsOpt.get
    updatedAgentSettings.priority += value
    zkService.setData(s"/producers/agents/$streamName/$partition/" + thisAgentPath, updatedAgentSettings)
    logger.debug(s"[PRIOR] Finish amend agent priority with value:{$value} with address:{$agentAddress}" +
      s" on stream:{$streamName},partition:{$partition} VALUENOW={${updatedAgentSettings.priority}}\n")
  }

  /**
    * Helper method for new master voting
    *
    * @param partition New master partition
    * @param retries   Retries to try to set new master
    * @return Selected master address
    */
  private def startVotingInternal(partition: Int, retries: Int = zkRetriesAmount): String = {
    logger.debug(s"[VOTING] Start voting new agent on address:{$agentAddress}" +
      s" on stream:{$streamName},partition:{$partition}\n")
    val master = getMaster(partition)
    master.fold{
      val agentsOpt = zkService.getAllSubNodesData[AgentSettings](s"/producers/agents/$streamName/$partition")
      assert(agentsOpt.isDefined)
      val agents = agentsOpt.get.sortBy(x => x.priority - x.penalty)
      val bestMaster = agents.last.agentAddress
      transport.setMasterRequest(SetMasterRequest(agentAddress, bestMaster, partition), transportTimeout) match {
        case null =>
          if (retries == 0)
            throw new IllegalStateException("agent is not responded")
          //assume that if master is not responded it will be deleted by zk
          Thread.sleep(1000)
          startVotingInternal(partition, retries - 1)

        case SetMasterResponse(_, _, p) =>
          assert(p == partition)
          bestMaster

        case EmptyResponse(_, _, p) =>
          assert(p == partition)
          bestMaster
      }
    }(master => master.agentAddress)
  }

  /**
    * Voting new master for concrete partition
    *
    * @param partition Partition to vote new master
    * @return New master Address
    */
  private def startVoting(partition: Int): String =
    LockUtil.withZkLockOrDieDo[String](zkService.getLock(s"/producers/lock_voting/$streamName/$partition"), (100, TimeUnit.SECONDS), Some(logger), () => {
      startVotingInternal(partition)
    })

  /**
    * Updating master on concrete partition
    *
    * @param partition Partition to update master
    * @param init      If flag true master will be reselected anyway else old master can stay
    * @param retries   Retries to try to interact with master
    */
  private def updateMaster(partition: Int, init: Boolean, retries: Int = zkRetriesAmount): Unit = {
    logger.debug(s"[UPDATER] Updating master with init:{$init} on agent:{$agentAddress}" +
      s" on stream:{$streamName},partition:{$partition} with retry=$retries\n")
    val masterOpt = getMaster(partition)
    masterOpt.fold[Unit](startVoting(partition)) { master =>
      if (init) {
        val ans = transport.deleteMasterRequest(DeleteMasterRequest(agentAddress, master.agentAddress, partition), transportTimeout)
        ans match {
          case null =>
            if (retries == 0)
              throw new IllegalStateException("agent is not responded")
            //assume that if master is not responded it will be deleted by zk
            Thread.sleep(1000)
            updateMaster(partition, init, retries - 1)

          case EmptyResponse(_, _, p) =>
            assert(p == partition)
            Thread.sleep(1000)
            updateMaster(partition, init, zkRetriesAmount)

          case DeleteMasterResponse(_, _, p) =>
            assert(p == partition)
            val newMaster = startVoting(partition)
            logger.debug(s"[UPDATER] Finish updating master with init:{$init} on agent:{$agentAddress}" +
              s" on stream:{$streamName},partition:{$partition} with retry=$retries; revoted master:{$newMaster}\n")
            localMasters.put(partition, newMaster)
        }
      } else {
        transport.pingRequest(PingRequest(agentAddress, master.agentAddress, partition), transportTimeout) match {
          case null =>
            if (retries == 0)
              throw new IllegalStateException("agent is not responded")
            //assume that if master is not responded it will be deleted by zk
            Thread.sleep(1000)
            updateMaster(partition, init, retries - 1)

          case EmptyResponse(_, _, p) =>
            assert(p == partition)
            Thread.sleep(1000)
            updateMaster(partition, init, zkRetriesAmount)

          case PingResponse(_, _, p) =>
            assert(p == partition)
            logger.debug(s"[UPDATER] Finish updating master with init:{$init} on agent:{$agentAddress}" +
              s" on stream:{$streamName},partition:{$partition} with retry=$retries; old master:{$master} is alive now\n")
            localMasters.put(partition, master.agentAddress)
        }
      }
    }
  }

  /**
    * Return master for concrete partition
    *
    * @param partition Partition to set
    * @return Master address
    */
  private def getMaster(partition: Int): Option[MasterSettings] =
    LockUtil.withLockOrDieDo[Option[MasterSettings]](lockManagingMaster, (100, TimeUnit.SECONDS), Some(logger), () => {
      LockUtil.withZkLockOrDieDo[Option[MasterSettings]](zkService.getLock(s"/producers/lock_master/$streamName/$partition"), (100, TimeUnit.SECONDS), Some(logger), () => {
        val masterOpt = zkService.get[MasterSettings](s"/producers/master/$streamName/$partition")
        logger.debug(s"[GET MASTER]Agent:{${masterOpt.getOrElse("None")}} is current master on" +
          s" stream:{$streamName},partition:{$partition}\n")
        masterOpt
      })
    })

  /**
    * Set this agent as new master on concrete partition
    *
    * @param partition Partition to set
    */
  def setThisAgentAsMaster(partition: Int) =
    LockUtil.withLockOrDieDo[Unit](lockManagingMaster, (100, TimeUnit.SECONDS), Some(logger), () => {
      LockUtil.withZkLockOrDieDo[Unit](zkService.getLock(s"/producers/lock_master/$streamName/$partition"), (100, TimeUnit.SECONDS), Some(logger), () => {
        assert(!zkService.exist(s"/producers/master/$streamName/$partition"))
        val masterSettings = MasterSettings(agentAddress, uniqueAgentId)
        zkService.create[MasterSettings](s"/producers/master/$streamName/$partition", masterSettings, CreateMode.EPHEMERAL)
        logger.debug(s"[SET MASTER]Agent:{$agentAddress} in master now on" +
          s" stream:{$streamName},partition:{$partition}\n")
      })
    })


  /**
    * Unset this agent as master on concrete partition
    *
    * @param partition Partition to set
    */
  def deleteThisAgentFromMasters(partition: Int) =
    LockUtil.withLockOrDieDo[Unit](lockManagingMaster, (100, TimeUnit.SECONDS), Some(logger), () => {
      LockUtil.withZkLockOrDieDo[Unit](zkService.getLock(s"/producers/lock_master/$streamName/$partition"), (100, TimeUnit.SECONDS), Some(logger), () => {
        zkService.delete(s"/producers/master/$streamName/$partition")
        logger.debug(s"[DELETE MASTER]Agent:{$agentAddress} in NOT master now on" +
          s" stream:{$streamName},partition:{$partition}\n")
      })
    })

  /**
    * Starting validate zk connection (if it will be down, exception will be thrown)
    * Probably may be removed
    */
  private def startSessionKeepAliveThread() = {
    val latch = new CountDownLatch(1)
    zkConnectionValidator = new Thread(new Runnable {
      override def run(): Unit = {
        latch.countDown()
        var retries = 0
        while (isRunning.get()) {
          if (!zkService.isZkConnected)
            retries += 1
          else
            retries = 0
          if (retries >= 3) {
            println("Zk connection Lost\n")
            //TODO replace somehow System.exit with exception
            System.exit(1)
          }
          Thread.sleep(1000)
        }
      }
    })
    zkConnectionValidator.start()
    latch.await()
  }

  /**
    * Retrieve new transaction from agent
    *
    * @param partition Transaction partition
    * @return Transaction UUID
    */
  def generateNewTransaction(partition: Int): UUID = {
    LockUtil.withLockOrDieDo[UUID](externalAccessLock, (100, TimeUnit.SECONDS), Some(logger), () => {
      val master = localMasters.getOrDefault(partition, null)
      logger.debug(s"[GETTXN] Start retrieve txn for agent with address:{$agentAddress}," +
        s"stream:{$streamName},partition:{$partition} from [MASTER:{$master}]\n")

      val res =
        if (master != null) {
          val txnResponse = transport.transactionRequest(TransactionRequest(agentAddress, master, partition), transportTimeout)
          txnResponse match {
            case null =>
              updateMaster(partition, init = false)
              generateNewTransaction(partition)

            case EmptyResponse(snd, rcv, p) =>
              assert(p == partition)
              updateMaster(partition, init = false)
              generateNewTransaction(partition)

            case TransactionResponse(snd, rcv, uuid, p) =>
              assert(p == partition)
              logger.debug(s"[GETTXN] Finish retrieve txn for agent with address:{$agentAddress}," +
                s"stream:{$streamName},partition:{$partition} with timeuuid:{${uuid.timestamp()}} from [MASTER:{$master}]s")
              uuid
          }
        } else {
          updateMaster(partition, init = false)
          generateNewTransaction(partition)
        }
      res
    })
  }

  //TODO remove after complex testing
  def publish(msg: Message): Unit = {
    LockUtil.withLockOrDieDo[Unit](externalAccessLock, (100, TimeUnit.SECONDS), Some(logger), () => {
      assert(msg.status != TransactionStatus.update)
      val master = localMasters.getOrDefault(msg.partition, null)
      logger.debug(s"[PUBLISH] SEND PTM:{$msg} to [MASTER:{$master}] from agent:{$agentAddress}," +
        s"stream:{$streamName}\n")

      if (master != null) {
        val txnResponse = transport.publishRequest(PublishRequest(agentAddress, master, msg), transportTimeout)
        txnResponse match {
          case null =>
            updateMaster(msg.partition, init = false)
            publish(msg)

          case EmptyResponse(snd, rcv, p) =>
            assert(p == msg.partition)
            updateMaster(msg.partition, init = false)
            publish(msg)

          case PublishResponse(snd, rcv, m) =>
            assert(msg.partition == m.partition)
            logger.debug(s"[PUBLISH] PUBLISHED PTM:{$msg} to [MASTER:{$master}] from agent:{$agentAddress}," +
              s"stream:{$streamName}\n")
        }
      } else {
        updateMaster(msg.partition, init = false)
        publish(msg)
      }
    })
  }

  /**
    * Stop this agent
    */
  def stop() =
    LockUtil.withLockOrDieDo[Unit](externalAccessLock, (100, TimeUnit.SECONDS), Some(logger), () => {
      isRunning.set(false)
      zkConnectionValidator.join()
      //to avoid infinite polling block
      transport.stopRequest(EmptyRequest(agentAddress, agentAddress, usedPartitions.head))
      messageHandler.join()
      transport.unbindLocalAddress()
      zkService.close()
    })

  /**
    * Start handling incoming messages for this agent
    */
  private def beginHandleMessages() = {
    val latch = new CountDownLatch(1)
    val agent = this
    messageHandler = new Thread(new Runnable {
      override def run(): Unit = {
        val executors = mutable.Map[Int, FirstFailLockableTaskExecutor]()
        (0 until poolSize) foreach { x =>
          executors(x) = new FirstFailLockableTaskExecutor
        }
        val partitionsToExecutors = usedPartitions
          .zipWithIndex
          .map { case (partition, execNum) => (partition, execNum % poolSize) }
          .toMap

        latch.countDown()

        while (isRunning.get()) {
          val request: IMessage = transport.waitRequest()
          logger.debug(s"[HANDLER] Start handle msg:{$request} on agent:{$agentAddress}")
          val task: Runnable = new Runnable {
              override def run(): Unit = request.handleP2PRequest(agent)
            }

          assert(partitionsToExecutors.contains(request.partition))
          val execNum = partitionsToExecutors(request.partition)
          executors(execNum).execute(task)
        }
        //graceful shutdown all executors after finishing message handling
        executors.foreach(x => x._2.shutdownSafe())
      }
    })
    messageHandler.start()
    latch.await()
  }

}