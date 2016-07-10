package com.bwsw.tstreams.coordination.transactions.peertopeer

import java.net.InetSocketAddress
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{CountDownLatch, ExecutorService, Executors}

import com.bwsw.tstreams.agents.producer.BasicProducer
import com.bwsw.tstreams.common.zkservice.ZkService
import com.bwsw.tstreams.coordination.pubsub.messages.{ProducerTopicMessage, ProducerTransactionStatus}
import com.bwsw.tstreams.coordination.transactions.messages._
import com.bwsw.tstreams.coordination.transactions.transport.traits.ITransport
import org.apache.zookeeper.CreateMode
import org.slf4j.LoggerFactory


/**
 * Agent for providing peer to peer interaction between [[BasicProducer]]]
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
class PeerToPeerAgent(agentAddress : String,
                      zkHosts : List[InetSocketAddress],
                      zkRootPath : String,
                      zkSessionTimeout: Int,
                      zkConnectionTimeout : Int,
                      producer : BasicProducer[_,_],
                      usedPartitions : List[Int],
                      isLowPriorityToBeMaster : Boolean,
                      transport: ITransport,
                      transportTimeout : Int,
                      poolSize : Int) {

  private val logger = LoggerFactory.getLogger(this.getClass)
  private val zkRetriesAmount = 60
  private val zkService = new ZkService(zkRootPath, zkHosts, zkSessionTimeout, zkConnectionTimeout)
  private val localMasters = scala.collection.mutable.Map[Int/*partition*/, String/*master*/]()
  private val lockLocalMasters = new ReentrantLock(true)
  private val lockManagingMaster = new ReentrantLock(true)
  private val streamName = producer.stream.getName
  private val isRunning = new AtomicBoolean(true)
  private var zkConnectionValidator : Thread = null
  private var messageHandler : Thread = null

  logger.debug(s"[INIT] Start initialize agent with address:{$agentAddress}," +
    s"stream:{$streamName},partitions:{${usedPartitions.mkString(",")}}\n")
  usedPartitions foreach {p =>
    tryCleanAgent(p)
  }
  transport.bindLocalAddress(agentAddress)
  startValidator()
  startHandleMessages()
  usedPartitions foreach {p =>
    val penalty = if (isLowPriorityToBeMaster) 1000*1000 else 0
    zkService.create[AgentSettings](s"/producers/agents/$streamName/$p/agent_${agentAddress}_",
       AgentSettings(agentAddress, priority = 0, penalty),
       CreateMode.EPHEMERAL_SEQUENTIAL)
  }
  usedPartitions foreach { p =>
    updateMaster(p, init = true)
  }
  logger.debug(s"[INIT] Finish initialize agent with address:{$agentAddress}," +
    s"stream:{$streamName},partitions:{${usedPartitions.mkString(",")}\n")

  /**
   * Validate that agent with [[agentAddress]]] not exist and delete in opposite
 *
   * @param partition Partition to check
   */
  private def tryCleanAgent(partition : Int) : Unit = {
    val agentsOpt = zkService.getAllSubPath(s"/producers/agents/$streamName/$partition")
    if (agentsOpt.isEmpty)
      return
    val agents: List[String] = agentsOpt.get
    val filtered = agents.filter(_ contains s"_${agentAddress}_")
    filtered foreach { path =>
      logger.debug(s"[INIT CLEAN] Delete agent on address:{$path} on" +
        s"stream:{$streamName},partition:{$partition}\n")
      zkService.delete(s"/producers/agents/$streamName/$partition/" + path)
      logger.debug(s"[INIT CLEAN FINISHED] Delete agent on address:{$path} on" +
        s"stream:{$streamName},partition:{$partition}\n")
    }
  }

  /**
   * Amend agent priority
 *
   * @param partition Partition to update priority
   * @param value Value which will be added to current priority
   */
  private def updateThisAgentPriority(partition : Int, value : Int) = {
    logger.debug(s"[PRIOR] Start amend agent priority with value:{$value} with address:{$agentAddress}" +
      s" on stream:{$streamName},partition:{$partition}\n")
    val agentsOpt = zkService.getAllSubPath(s"/producers/agents/$streamName/$partition")
    assert(agentsOpt.isDefined)
    val agents: List[String] = agentsOpt.get
    val filtered = agents.filter(_ contains s"_${agentAddress}_")
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
   * @param retries Retries to try to set new master
   * @return Selected master address
   */
  private def startVotingInternal(partition : Int, retries : Int = zkRetriesAmount) : String = {
    logger.debug(s"[VOTING] Start voting new agent on address:{$agentAddress}" +
      s" on stream:{$streamName},partition:{$partition}\n")
    val masterID = getMaster(partition)
    val newMaster : String = {
      if (masterID.isDefined)
        masterID.get
      else {
        val agentsOpt = zkService.getAllSubNodesData[AgentSettings](s"/producers/agents/$streamName/$partition")
        assert(agentsOpt.isDefined)
        val agents = agentsOpt.get.sortBy(x=>x.priority-x.penalty)
        val bestMaster = agents.last.id
        transport.setMasterRequest(SetMasterRequest(agentAddress, bestMaster, partition), transportTimeout) match {
          case null =>
            if (retries == 0)
              throw new IllegalStateException("agent is not responded")
            //assume that if master is not responded it will be deleted by zk
            Thread.sleep(1000)
            startVotingInternal(partition, retries - 1)

          case SetMasterResponse(_,_,p) =>
            assert(p == partition)
            bestMaster

          case EmptyResponse(_,_,p) =>
            assert(p == partition)
            bestMaster
        }
      }
    }
    logger.debug(s"[VOTING] Finish voting new agent on address:{$agentAddress}" +
      s" on stream:{$streamName},partition:{$partition}, voted master:{$newMaster}\n")
    newMaster
  }

  /**
   * Voting new master for concrete partition
 *
   * @param partition Partition to vote new master
   * @return New master Address
   */
  private def startVoting(partition : Int) : String = {
    val lock = zkService.getLock(s"/producers/lock_voting/$streamName/$partition")
    lock.lock()
    val newMaster = startVotingInternal(partition)
    lock.unlock()
    newMaster
  }

  /**
   * Updating master on concrete partition
 *
   * @param partition Partition to update master
   * @param init If flag true master will be reselected anyway else old master can stay
   * @param retries Retries to try to interact with master
   */
  private def updateMaster(partition : Int, init : Boolean, retries : Int = zkRetriesAmount) : Unit = {
    logger.debug(s"[UPDATER] Updating master with init:{$init} on agent:{$agentAddress}" +
      s" on stream:{$streamName},partition:{$partition} with retry=$retries\n")
    val masterOpt = getMaster(partition)
    if (masterOpt.isDefined) {
      val master = masterOpt.get
      if (init) {
        val ans = transport.deleteMasterRequest(DeleteMasterRequest(agentAddress, master, partition), transportTimeout)
        ans match {
          case null =>
            if (retries == 0)
              throw new IllegalStateException("agent is not responded")
            //assume that if master is not responded it will be deleted by zk
            Thread.sleep(1000)
            updateMaster(partition, init, retries-1)

          case EmptyResponse(_, _, p) =>
            assert(p == partition)
            Thread.sleep(1000)
            updateMaster(partition, init, zkRetriesAmount)

          case DeleteMasterResponse(_, _, p) =>
            assert(p == partition)
            val newMaster = startVoting(partition)
            logger.debug(s"[UPDATER] Finish updating master with init:{$init} on agent:{$agentAddress}" +
              s" on stream:{$streamName},partition:{$partition} with retry=$retries; revoted master:{$newMaster}\n")
            lockLocalMasters.lock()
            localMasters(partition) = newMaster
            lockLocalMasters.unlock()
        }

      } else {
        transport.pingRequest(PingRequest(agentAddress, master, partition), transportTimeout) match {
          case null =>
            if (retries == 0)
              throw new IllegalStateException("agent is not responded")
            //assume that if master is not responded it will be deleted by zk
            Thread.sleep(1000)
            updateMaster(partition, init, retries-1)

          case EmptyResponse(_,_, p) =>
            assert(p == partition)
            Thread.sleep(1000)
            updateMaster(partition, init, zkRetriesAmount)

          case PingResponse(_,_,p) =>
            assert(p == partition)
            logger.debug(s"[UPDATER] Finish updating master with init:{$init} on agent:{$agentAddress}" +
              s" on stream:{$streamName},partition:{$partition} with retry=$retries; old master:{$master} is alive now\n")
            lockLocalMasters.lock()
            localMasters(partition) = master
            lockLocalMasters.unlock()
        }
      }
    }
    else {
      startVoting(partition)
    }
  }

  /**
   * Return master for concrete partition
 *
   * @param partition Partition to set
   * @return Master address
   */
  private def getMaster(partition : Int) : Option[String] = {
    lockManagingMaster.lock()
    val lock = zkService.getLock(s"/producers/lock_master/$streamName/$partition")
    lock.lock()
    val masterOpt = zkService.get[String](s"/producers/master/$streamName/$partition")
    lock.unlock()
    lockManagingMaster.unlock()
    logger.debug(s"[GET MASTER]Agent:{${masterOpt.getOrElse("None")}} is current master on" +
      s" stream:{$streamName},partition:{$partition}\n")
    masterOpt
  }

  /**
   * Set this agent as new master on concrete partition
 *
   * @param partition Partition to set
   */
  private def setThisAgentAsMaster(partition : Int) : Unit = {
    lockManagingMaster.lock()
    val lock = zkService.getLock(s"/producers/lock_master/$streamName/$partition")
    lock.lock()
    //TODO remove after debug
    assert(!zkService.exist(s"/producers/master/$streamName/$partition"))
    zkService.create(s"/producers/master/$streamName/$partition", agentAddress, CreateMode.EPHEMERAL)
    lock.unlock()
    lockManagingMaster.unlock()
    logger.debug(s"[SET MASTER]Agent:{$agentAddress} in master now on" +
      s" stream:{$streamName},partition:{$partition}\n")
  }

  /**
   * Unset this agent as master on concrete partition
 *
   * @param partition Partition to set
   */
  private def deleteThisAgentFromMasters(partition : Int) : Unit = {
    lockManagingMaster.lock()
    val lock = zkService.getLock(s"/producers/lock_master/$streamName/$partition")
    lock.lock()
    zkService.delete(s"/producers/master/$streamName/$partition")
    lock.unlock()
    lockManagingMaster.unlock()
    logger.debug(s"[DELETE MASTER]Agent:{$agentAddress} in NOT master now on" +
      s" stream:{$streamName},partition:{$partition}\n")
  }

  /**
   * Starting validate zk connection(if it will be down, exception will be thrown)
   */
  private def startValidator() = {
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
            //TODO replace System.exit with exception
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
  def getNewTxn(partition : Int) : UUID = {
    lockLocalMasters.lock()
    val condition = localMasters.contains(partition)
    val localMaster = if (condition) localMasters(partition) else null
    lockLocalMasters.unlock()
    logger.debug(s"[GETTXN] Start retrieve txn for agent with address:{$agentAddress}," +
      s"stream:{$streamName},partition:{$partition} from [MASTER:{$localMaster}]\n")
    if (condition){
      val txnResponse = transport.transactionRequest(TransactionRequest(agentAddress, localMaster, partition), transportTimeout)
      txnResponse match {
        case null =>
          updateMaster(partition, init = false)
          getNewTxn(partition)

        case EmptyResponse(snd,rcv,p) =>
          assert(p == partition)
          updateMaster(partition, init = false)
          getNewTxn(partition)

        case TransactionResponse(snd, rcv, uuid, p) =>
          assert(p == partition)
          logger.debug(s"[GETTXN] Finish retrieve txn for agent with address:{$agentAddress}," +
            s"stream:{$streamName},partition:{$partition} with timeuuid:{${uuid.timestamp()}} from [MASTER:{$localMaster}]\n")
          uuid
      }
    } else {
      updateMaster(partition, init = false)
      getNewTxn(partition)
    }
  }

  //TODO remove after complex testing
  def publish(msg : ProducerTopicMessage) : Unit = {
    assert(msg.status != ProducerTransactionStatus.updated)
    lockLocalMasters.lock()
    val condition = localMasters.contains(msg.partition)
    val localMaster = if (condition) localMasters(msg.partition) else null
    lockLocalMasters.unlock()
    logger.debug(s"[PUBLISH] SEND PTM:{$msg} to [MASTER:{$localMaster}] from agent:{$agentAddress}," +
      s"stream:{$streamName}\n")
    if (condition){
      val txnResponse = transport.publishRequest(PublishRequest(agentAddress, localMaster, msg), transportTimeout)
      txnResponse match {
        case null =>
          updateMaster(msg.partition, init = false)
          publish(msg)

        case EmptyResponse(snd,rcv,p) =>
          assert(p == msg.partition)
          updateMaster(msg.partition, init = false)
          publish(msg)

        case PublishResponse(snd, rcv, m) =>
          assert(msg.partition == m.partition)
          logger.debug(s"[PUBLISH] PUBLISHED PTM:{$msg} to [MASTER:{$localMaster}] from agent:{$agentAddress}," +
            s"stream:{$streamName}\n")
      }
    } else {
      updateMaster(msg.partition, init = false)
      publish(msg)
    }
  }

  /**
   * Stop this agent
   */
  def stop() = {
    isRunning.set(false)
    zkConnectionValidator.join()
    //to avoid infinite polling block
    transport.stopRequest(EmptyRequest(agentAddress, agentAddress, usedPartitions.head))
    messageHandler.join()
    transport.unbindLocalAddress()
    zkService.close()
  }

  /**
   * Start handling incoming messages for this agent
   */
  private def startHandleMessages() = {
    val latch = new CountDownLatch(1)
    messageHandler = new Thread(new Runnable {
      override def run(): Unit = {
        latch.countDown()
        val executors = scala.collection.mutable.Map[Int, ExecutorService]()
        (0 until poolSize) foreach { x =>
          executors(x) = Executors.newSingleThreadExecutor()
        }
        val partitionsToExecutors = usedPartitions
          .zipWithIndex
          .map{case(partition,execNum)=>(partition,execNum % poolSize)}
          .toMap

        while (isRunning.get()) {
          val request: IMessage = transport.waitRequest()
          logger.debug(s"[HANDLER] Start handle msg:{$request} on agent:{$agentAddress}")
          val task : Runnable = createTask(request)

          assert(partitionsToExecutors.contains(request.partition))
          val execNum = partitionsToExecutors(request.partition)
          executors(execNum).execute(task)
        }
        //graceful shutdown all executors after finishing message handling
        executors.foreach(x=>x._2.shutdown())
      }
    })
    messageHandler.start()
    latch.await()
  }

  /**
   * Create task to handle incoming message
 *
   * @param request Requested message
   * @return Response
   */
  private def createTask(request : IMessage): Runnable = {
    new Runnable {
      override def run(): Unit = {
        request match {
          case PingRequest(snd, rcv, partition) =>
            lockLocalMasters.lock()
            assert(rcv == agentAddress)
            val response = {
              if (localMasters.contains(partition) && localMasters(partition) == agentAddress)
                PingResponse(rcv, snd, partition)
              else
                EmptyResponse(rcv, snd, partition)
            }
            lockLocalMasters.unlock()
            response.msgID = request.msgID
            transport.response(response)

          case SetMasterRequest(snd, rcv, partition) =>
            lockLocalMasters.lock()
            assert(rcv == agentAddress)
            val response = {
              if (localMasters.contains(partition) && localMasters(partition) == agentAddress)
                EmptyResponse(rcv,snd,partition)
              else {
                localMasters(partition) = agentAddress
                setThisAgentAsMaster(partition)
                usedPartitions foreach { partition =>
                  updateThisAgentPriority(partition, value = -1)
                }
                SetMasterResponse(rcv, snd, partition)
              }
            }
            lockLocalMasters.unlock()
            response.msgID = request.msgID
            transport.response(response)

          case DeleteMasterRequest(snd, rcv, partition) =>
            lockLocalMasters.lock()
            assert(rcv == agentAddress)
            val response = {
              if (localMasters.contains(partition) && localMasters(partition) == agentAddress) {
                localMasters.remove(partition)
                deleteThisAgentFromMasters(partition)
                usedPartitions foreach { partition =>
                  updateThisAgentPriority(partition, value = 1)
                }
                DeleteMasterResponse(rcv, snd, partition)
              } else
                EmptyResponse(rcv, snd, partition)
            }
            lockLocalMasters.unlock()
            response.msgID = request.msgID
            transport.response(response)

          case TransactionRequest(snd, rcv, partition) =>
            lockLocalMasters.lock()
            assert(rcv == agentAddress)
            val response = {
              if (localMasters.contains(partition) && localMasters(partition) == agentAddress) {
                val txnUUID: UUID = producer.getLocalTxn(partition)
                TransactionResponse(rcv, snd, txnUUID, partition)
              } else
                EmptyResponse(rcv, snd, partition)
            }
            lockLocalMasters.unlock()
            response.msgID = request.msgID
            transport.response(response)

          case PublishRequest(snd, rcv, msg) =>
            lockLocalMasters.lock()
            assert(rcv == agentAddress)
            val response = {
              if (localMasters.contains(msg.partition) && localMasters(msg.partition) == agentAddress) {
                producer.coordinator.publish(msg)
                PublishResponse(rcv, snd,
                  ProducerTopicMessage(UUID.randomUUID(),0,ProducerTransactionStatus.opened,msg.partition))
              } else
                EmptyResponse(rcv, snd, msg.partition)
            }
            lockLocalMasters.unlock()
            response.msgID = request.msgID
            transport.response(response)

          case EmptyRequest(snd,rcv,p) =>
            val response = EmptyResponse(rcv,snd,p)
            response.msgID = request.msgID
            transport.response(response)
        }
      }
    }
  }
}