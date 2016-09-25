package com.bwsw.tstreams.coordination.producer

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.producer.Producer
import com.bwsw.tstreams.common.ProtocolMessageSerializer.ProtocolMessageSerializerException
import com.bwsw.tstreams.common.{LockUtil, ProtocolMessageSerializer, ZookeeperDLMService}
import com.bwsw.tstreams.coordination.client.TcpTransport
import com.bwsw.tstreams.coordination.messages.master._
import com.bwsw.tstreams.coordination.messages.state.TransactionStateMessage
import io.netty.channel.Channel
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.Random

/**
  *
  */
object PeerAgent {
  /**
    * For making low priority masters
    *
    */
  val LOW_PRIORITY_PENALTY = 1000 * 1000
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
  * @param zkService
  * @param peerKeepAliveTimeout
  * @param producer                Producer reference
  * @param usedPartitions          List of used producer partitions
  * @param isLowPriorityToBeMaster Flag which indicate to have low priority to be master
  * @param transport               Transport to provide interaction
  */
class PeerAgent(agentsStateManager: AgentsStateDBService,
                zkService: ZookeeperDLMService,
                peerKeepAliveTimeout: Int,
                producer: Producer[_],
                usedPartitions: List[Int],
                isLowPriorityToBeMaster: Boolean,
                transport: TcpTransport,
                threadPoolAmount: Int,
                threadPoolPublisherThreadsAmount: Int,
                partitionRedistributionDelay: Int,
                isMasterBootstrapModeFull: Boolean,
                isMasterProcessVote: Boolean) {

  val myInetAddress: String = producer.producerOptions.coordinationOptions.transport.getInetAddress()

  val awaitPartitionRedistributionThreadComplete = new CountDownLatch(1)

  /**
    * Job Executors
    */
  private val executorGraphs = mutable.Map[Int, ExecutorGraph]()

  private val streamName = producer.stream.getName
  private val isRunning = new AtomicBoolean(true)

  private var zkConnectionValidator: Thread = null
  private var partitionWeightDistributionThread: Thread = null

  def getAgentAddress() = myInetAddress

  def getProducer() = producer

  def getAgentsStateManager() = agentsStateManager

  /**
    * this ID is used to track sequential transactions from the same master
    */
  private val uniqueAgentId = Math.abs(Random.nextInt() + 1)

  def getUniqueAgentID() = uniqueAgentId

  def getAndIncSequentialID(partition: Int): Long = sequentialIds(partition).getAndIncrement()

  /**
    * this ID map is used to track sequential transactions on subscribers
    */
  private val sequentialIds = mutable.Map[Int, AtomicLong]()

  PeerAgent.logger.info(s"[INIT] Start initialize agent with address: {$myInetAddress}")
  PeerAgent.logger.info(s"[INIT] Stream: {$streamName}, partitions: [${usedPartitions.mkString(",")}]")
  PeerAgent.logger.info(s"[INIT] Master Unique random ID: $uniqueAgentId")

  transport.start((s: Channel, m: String) => this.handleMessage(s, m))

  startSessionKeepAliveThread()

  (0 until threadPoolAmount) foreach { x =>
    executorGraphs(x) = new ExecutorGraph(s"id-$x", publisherThreadsAmount = threadPoolPublisherThreadsAmount)
  }

  private val partitionsToExecutors = usedPartitions
    .zipWithIndex
    .map { case (partition, execNum) => (partition, execNum % threadPoolAmount) }
    .toMap


  // fill initial sequential counters
  usedPartitions foreach { p => sequentialIds += (p -> new AtomicLong(0)) }

  agentsStateManager.doLocked {
    agentsStateManager.bootstrap(isLowPriorityToBeMaster, uniqueAgentId, isMasterBootstrapModeFull)
  }

  partitionWeightDistributionThread = new Thread(new Runnable {
    override def run(): Unit = {
      val pq = usedPartitions
      val it = pq.iterator
      while (isRunning.get() && it.hasNext) {
        Thread.sleep(partitionRedistributionDelay * 1000)
        val item = it.next
        updateMaster(item, init = true)
        PeerAgent.logger.info(s"Master update request for partition $item is complete.")
      }
      awaitPartitionRedistributionThreadComplete.countDown()
    }
  })
  if (isMasterProcessVote)
    partitionWeightDistributionThread.start()
  else
    awaitPartitionRedistributionThreadComplete.countDown()


  /**
    * Helper method for new master voting
    *
    * @param partition New master partition
    * @param now   ???
    * @param expiresAt ???
    * @return Selected master address
    */
  private def electPartitionMaster(partition: Int, now: Long, expiresAt: Long): MasterConfiguration = {
    PeerAgent.logger.info(s"[MASTER VOTE INIT] Start voting new agent on address: {$myInetAddress} on stream: {$streamName}, partition:{$partition}")

    val master = agentsStateManager.getCurrentMaster(partition)

    master.fold {

      val bestCandidate = agentsStateManager.getBestMasterCandidate(partition)

      if(bestCandidate.agentAddress == master)
        return bestCandidate

      transport.setMasterRequest(bestCandidate.agentAddress, partition) match {
        case null =>
          if (now > expiresAt)
            throw new IllegalStateException(s"Expected master hasn't occurred up to timestamp $expiresAt.")
          //assume that if master is not responded it will be deleted by zk
          Thread.sleep(PeerAgent.RETRY_SLEEP_TIME)
          electPartitionMaster(partition, System.currentTimeMillis(), expiresAt)

        case SetMasterResponse(_, _, p) =>
          assert(p == partition)
          bestCandidate

        case EmptyResponse(_, _, p) =>
          assert(p == partition)
          bestCandidate
      }
    }(master => master)
  }

  def updateMaster(partition: Int, init: Boolean): Unit = this.synchronized {
    agentsStateManager doLocked {
      updateMasterInternal(partition, init, System.currentTimeMillis(), System.currentTimeMillis() + peerKeepAliveTimeout)
    }
  }

  /**
    * Updating master on concrete partition
    *
    * @param partition Partition to update master
    * @param isDemoteCurrentMaster      If flag true master will be reselected anyway else old master can stay
    * @param expiresAt   Retries to try to interact with master
    */
  def updateMasterInternal(partition: Int, isDemoteCurrentMaster: Boolean, now: Long, expiresAt: Long): Unit = {
    PeerAgent.logger.info(s"[MASTER UPDATE INIT] Updating master with init=$isDemoteCurrentMaster on agent: {$myInetAddress} on stream: {$streamName}, partition: {$partition} with retry=$expiresAt.")

    // nothing to do if I'm the master already
    // I don't vote for NOT being master.
    val masterOpt = agentsStateManager.getCurrentMaster(partition)
    if (masterOpt.fold(false)(master => master.agentAddress == myInetAddress))
      return

    masterOpt.fold[Unit](electPartitionMaster(partition, System.currentTimeMillis(), expiresAt)) { master =>
      if (isDemoteCurrentMaster) {
          transport.deleteMasterRequest(master.agentAddress, partition) match {
          case null =>
            if (now > expiresAt)
              throw new IllegalStateException(s"Agent ${master.agentAddress} didn't respond to me.")
            //assume that if master is not responded it will be deleted by zk
            Thread.sleep(PeerAgent.RETRY_SLEEP_TIME)
            updateMasterInternal(partition, isDemoteCurrentMaster, System.currentTimeMillis(), expiresAt)

          case EmptyResponse(_, _, p) =>
            assert(p == partition)
            Thread.sleep(PeerAgent.RETRY_SLEEP_TIME)
            updateMasterInternal(partition, isDemoteCurrentMaster, System.currentTimeMillis(), expiresAt)

          case DeleteMasterResponse(_, _, p) =>
            assert(p == partition)
            val newMaster = electPartitionMaster(partition, System.currentTimeMillis(), expiresAt)
            PeerAgent.logger.info(s"[MASTER UPDATE RESULT] Finished updating master with init=true on agent: {$myInetAddress} on stream: {$streamName}, partition: {$partition} with retry=$expiresAt; re-voted master: {$newMaster}.")
            agentsStateManager.putPartitionMasterLocally(partition, newMaster)
        }
      } else {
        transport.pingRequest(master.agentAddress, partition) match {
          case null =>
            if (now > expiresAt)
              throw new IllegalStateException(s"Agent ${master.agentAddress} didn't respond to me.")
            //assume that if master is not responded it will be deleted by zk
            Thread.sleep(PeerAgent.RETRY_SLEEP_TIME)
            updateMasterInternal(partition, isDemoteCurrentMaster, System.currentTimeMillis(), expiresAt)

          case EmptyResponse(_, _, p) =>
            assert(p == partition)
            Thread.sleep(PeerAgent.RETRY_SLEEP_TIME)
            updateMasterInternal(partition, isDemoteCurrentMaster, System.currentTimeMillis(), expiresAt)

          case PingResponse(_, _, p) =>
            assert(p == partition)
            PeerAgent.logger.info(s"[MASTER UPDATE RESULT] Finished updating master with init=false on agent: {$myInetAddress} on stream: {$streamName}, partition: {$partition} with retry=$expiresAt; old master: {$master} is alive now.")
            agentsStateManager.putPartitionMasterLocally(partition, master)
        }
      }
    }
  }


  /**
    * Starting validate zk connection (if it will be down, exception will be thrown)
    * Probably may be removed
    */
  private def startSessionKeepAliveThread() = {
    val latch = new CountDownLatch(1)
    val agent = this
    zkConnectionValidator = new Thread(new Runnable {
      override def run(): Unit = {
        latch.countDown()
        var retries = 0
        try {
          while (isRunning.get()) {
            if (!zkService.isZkConnected)
              retries += 1
            else
              retries = 0
            if (retries >= 3) {
              PeerAgent.logger.error(s"Agent ${agent.getProducer.name} $myInetAddress - Zk connection Lost. Immediately shutdown.")
              System.exit(1)
            }
            Thread.sleep(PeerAgent.RETRY_SLEEP_TIME)
          }
        } catch {
          case e: Exception =>
            PeerAgent.logger.error(s"Agent ${agent.getProducer.name} $myInetAddress - Zk connection Lost. Immediately shutdown.")
            System.exit(1)
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
    * @return TransactionID
    */
  def generateNewTransaction(partition: Int): Long = this.synchronized {
    val master = agentsStateManager.getPartitionMasterInetAddressLocal(partition, null)
    if (PeerAgent.logger.isDebugEnabled) {
      PeerAgent.logger.debug(s"[GET TRANSACTION] Start retrieve transaction for agent with address: {$myInetAddress}, stream: {$streamName}, partition: {$partition} from [MASTER: {$master}].")
    }

    val res =
      if (master != null) {
        transport.transactionRequest(master, partition) match {
        case null =>
          updateMaster(partition, init = false)
          generateNewTransaction(partition)

        case EmptyResponse(snd, rcv, p) =>
          assert(p == partition)
          updateMaster(partition, init = false)
          generateNewTransaction(partition)

        case TransactionResponse(snd, rcv, id, p) =>
          assert(p == partition)
          if (PeerAgent.logger.isDebugEnabled) {
            PeerAgent.logger.debug(s"[GET TRANSACTION] Finish retrieve transaction for agent with address: $myInetAddress, stream: $streamName, partition: $partition with ID: $id from [MASTER: $master]s")
          }
          id
        }
      } else {
        updateMaster(partition, init = false)
        generateNewTransaction(partition)
      }
    res
  }

  def notifyMaterialize(msg: TransactionStateMessage, to: String): Unit = {
    if (PeerAgent.logger.isDebugEnabled) {
      PeerAgent.logger.debug(s"[MATERIALIZE] Send materialize request address\nMe: $myInetAddress\nTransaction owner: $to\nStream: $streamName\npartition: ${msg.partition}\nTransaction: ${msg.transactionID}")
    }
    transport.materializeRequest(to, msg)
  }

  /**
    * Allows to publish update/pre/post/cancel messages.
    *
    * @param msg
    * @param isUpdateMaster
    * @return
    */
  def publish(msg: TransactionStateMessage, isUpdateMaster: Boolean = false): Boolean = {
    if (isUpdateMaster) {
      PeerAgent.logger.warn(s"Master update is requested for ${msg.partition}.")
      updateMaster(msg.partition, init = false)
    }
    val master = agentsStateManager.getPartitionMasterInetAddressLocal(msg.partition, null)

    if (PeerAgent.logger.isDebugEnabled)
      PeerAgent.logger.debug(s"[PUBLISH] SEND PTM:{$msg} to [MASTER:{$master}] from agent:{$myInetAddress}," +
        s"stream:{$streamName}")

    if (master != null) {
      transport.publishRequest(master, msg, onFailCallback = () => publish(msg, isUpdateMaster = true))
    } else {
      publish(msg, isUpdateMaster = true)
    }
  }

  /**
    * Stop this agent
    */
  def stop() = {
    PeerAgent.logger.info(s"P2PAgent of ${producer.name} is shutting down.")
    isRunning.set(false)

    if (isMasterProcessVote)
      partitionWeightDistributionThread.join()

    transport.stopServer()

    zkConnectionValidator.join()
    //to avoid infinite polling block
    executorGraphs.foreach(g => g._2.shutdown())

    agentsStateManager.shutdown()

    transport.stopClient()
  }

  def handleMessage(channel: Channel, rawMessage: String): Unit = {
    val agent = this
    try {
      val request: IMessage = ProtocolMessageSerializer.deserialize(rawMessage)
      request.channel = channel
      if (PeerAgent.logger.isDebugEnabled)
        PeerAgent.logger.debug(s"[HANDLER] Start handle msg:{$request} on agent:{$myInetAddress}")
      val task = () => request.run(agent)
      assert(partitionsToExecutors.contains(request.partition))
      val execNo = partitionsToExecutors(request.partition)
      request match {
        case _: PublishRequest => executorGraphs(execNo).submitToPublish("<PublishTask>", task)
        case _: NewTransactionRequest => executorGraphs(execNo).submitToNewTransaction("<NewTransactionTask>", task)
        case _: MaterializeRequest => executorGraphs(execNo).submitToMaterialize("<MaterializeTask>", task)
        case _ => executorGraphs(execNo).submitToGeneral("<GeneralTask>", task)
      }

    } catch {
      case e: ProtocolMessageSerializerException =>
        PeerAgent.logger.warn(s"Message '$rawMessage' cannot be deserialized. Exception is: ${e.getMessage}")
    }
  }

  /**
    * public method which allows to submit delayed task for execution
    *
    * @param task
    * @param partition
    */
  def submitPipelinedTaskToPublishExecutors(partition: Int, task: () => Unit) = {
    val execNum = partitionsToExecutors(partition)
    executorGraphs(execNum).submitToPublish("<PublishTask>", task)
  }

  /**
    * public method which allows to submit delayed task for execution
    *
    * @param task
    * @param partition
    */
  def submitPipelinedTaskToMaterializeExecutor(partition: Int, task: () => Unit) = {
    val execNum = partitionsToExecutors(partition)
    executorGraphs(execNum).submitToMaterialize("<MaterializeTask>", task)
  }

  /**
    * public method which allows to submit delayed task for execution
    *
    * @param task
    */
  def submitPipelinedTaskToCassandraExecutor(partition: Int, task: () => Unit) = {
    val execNum = partitionsToExecutors(partition)
    executorGraphs(execNum).submitToCassandra("<CassandraTask>", task)
  }

  /**
    * public method which allows to submit delayed task for execution
    *
    * @param task
    * @param partition
    */
  def submitPipelinedTaskToNewTransactionExecutors(partition: Int, task: () => Unit) = {
    val execNum = partitionsToExecutors(partition)
    executorGraphs(execNum).submitToNewTransaction("<NewTransactionTask>", task)
  }

  def getCassandraAsyncExecutor(partition: Int) =
    executorGraphs(partitionsToExecutors(partition)).getCassandra()
}