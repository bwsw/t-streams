package com.bwsw.tstreams.coordination.producer

import java.util.UUID
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.agents.producer.Producer
import com.bwsw.tstreams.common.ProtocolMessageSerializer.ProtocolMessageSerializerException
import com.bwsw.tstreams.common.{FirstFailLockableTaskExecutor, LockUtil, ProtocolMessageSerializer, ZookeeperDLMService}
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
  val LOW_PRIORITY_PENALTY  = 1000 * 1000
  /**
    * How long to sleep during master elections actions
    */
  val RETRY_SLEEP_TIME      = 1000
  /**
    * Generic logger object
    */
  val logger                = LoggerFactory.getLogger(this.getClass)

}

/**
 * Agent for providing peer to peer interaction between Producers
 *
 * @param zkService
 * @param zkRetriesAmount
 * @param producer Producer reference
 * @param usedPartitions List of used producer partitions
 * @param isLowPriorityToBeMaster Flag which indicate to have low priority to be master
 * @param transport Transport to provide interaction
 */
class PeerAgent(agentsStateManager: AgentsStateDBService,
                zkService: ZookeeperDLMService,
                zkRetriesAmount: Int,
                producer: Producer[_],
                usedPartitions: List[Int],
                isLowPriorityToBeMaster: Boolean,
                transport: TcpTransport,
                poolSize: Int) {
  val myInetAddress: String = producer.producerOptions.coordinationOptions.transport.getInetAddress()
  /**
    * locks
    */
  private val externalAccessLock = new ReentrantLock(true)

  /**
    * Job Executors
    */
  private val executorGraphs           = mutable.Map[Int, ExecutorGraph]()

  private val streamName = producer.stream.getName
  private val isRunning = new AtomicBoolean(true)

  private var zkConnectionValidator: Thread = null
  private var partitionWeightDistributionThread: Thread = null

  def getAgentAddress()           = myInetAddress
  def getTransport()              = transport
  def getUsedPartitions()         = usedPartitions
  def getProducer()               = producer
  def getAgentsStateManager()     = agentsStateManager

  /**
    * this ID is used to track sequential transactions from the same master
    */
  private val uniqueAgentId = Math.abs(Random.nextInt())

  def getUniqueAgentID() = uniqueAgentId
  def getAndIncSequentialID(partition: Int): Long = sequentialIds(partition).getAndIncrement()

  /**
    * this ID map is used to track sequential transactions on subscribers
    */
  private val sequentialIds = mutable.Map[Int, AtomicLong]()

  PeerAgent.logger.info(s"[INIT] Start initialize agent with address: {$myInetAddress}")
  PeerAgent.logger.info(s"[INIT] Stream: {$streamName}, partitions: [${usedPartitions.mkString(",")}]")
  PeerAgent.logger.info(s"[INIT] Master Unique random ID: $uniqueAgentId")

  transport.start((s: Channel, m: String) => this.handleMessage(s,m))

  startSessionKeepAliveThread()

  (0 until poolSize) foreach { x =>
    executorGraphs(x) = new ExecutorGraph(s"id-${x}")
  }

  private val partitionsToExecutors = usedPartitions
    .zipWithIndex
    .map { case (partition, execNum) => (partition, execNum % poolSize) }
    .toMap



  // fill initial sequential counters
  usedPartitions foreach { p =>sequentialIds += (p -> new AtomicLong(0)) }

  agentsStateManager.bootstrap(isLowPriorityToBeMaster, uniqueAgentId)


  partitionWeightDistributionThread = new Thread(new Runnable {
    override def run(): Unit = {
      val pq = usedPartitions.toIterable
      val it = pq.iterator
      while (isRunning.get() && it.hasNext) {
        updateMaster(it.next, init = true)
      }
    }
  })

  partitionWeightDistributionThread.start()
  //usedPartitions foreach { p =>
  //  updateMaster(p, init = true)
  //}

  /**
    * Helper method for new master voting
    *
    * @param partition New master partition
    * @param retries   Retries to try to set new master
    * @return Selected master address
    */
  private def electPartitionMasterInternal(partition: Int, retries: Int = zkRetriesAmount): String = {
    if (PeerAgent.logger.isDebugEnabled)
    {
      PeerAgent.logger.debug(s"[VOTING] Start voting new agent on address: {$myInetAddress} on stream: {$streamName}, partition:{$partition}")
    }
    val master = agentsStateManager.getCurrentMaster(partition)
    master.fold {
      val bestCandidate = agentsStateManager.getBestMasterCandidate(partition)
      transport.setMasterRequest(bestCandidate, partition) match {
        case null =>
          if (retries == 0)
            throw new IllegalStateException(s"Expected mastre didn't occure in ${zkRetriesAmount} trials.")
          //assume that if master is not responded it will be deleted by zk
          Thread.sleep(PeerAgent.RETRY_SLEEP_TIME)
          electPartitionMasterInternal(partition, retries - 1)

        case SetMasterResponse(_, _, p) =>
          assert(p == partition)
          bestCandidate

        case EmptyResponse(_, _, p) =>
          assert(p == partition)
          bestCandidate
      }
    }(master => master.agentAddress)
  }

  /**
    * Voting new master for concrete partition
    *
    * @param partition Partition to vote new master
    * @return New master Address
    */
  private def electPartitionMaster(partition: Int): String =
    agentsStateManager.withElectionLockDo(partition, () => electPartitionMasterInternal(partition))

  /**
    * Updating master on concrete partition
    *
    * @param partition Partition to update master
    * @param init      If flag true master will be reselected anyway else old master can stay
    * @param retries   Retries to try to interact with master
    */
  def updateMaster(partition: Int, init: Boolean, retries: Int = zkRetriesAmount): Unit = {
    if (PeerAgent.logger.isDebugEnabled)
    {
      PeerAgent.logger.debug(s"[UPDATER] Updating master with init: {$init} on agent: {$myInetAddress} on stream: {$streamName}, partition: {$partition} with retry=$retries.")
    }

    // nothing to do if I'm the master already
    // I don't vote for NOT being master.
    val masterOpt = agentsStateManager.getCurrentMaster(partition)
    if(masterOpt.isDefined) {
      if(masterOpt.get.agentAddress == myInetAddress)
        return
    }

    masterOpt.fold[Unit](electPartitionMaster(partition)) { master =>
      if (init) {
        val ans = transport.deleteMasterRequest(master.agentAddress, partition)
        ans match {
          case null =>
            if (retries == 0)
              throw new IllegalStateException(s"Agent didn't respond to me.")
            //assume that if master is not responded it will be deleted by zk
            Thread.sleep(PeerAgent.RETRY_SLEEP_TIME)
            updateMaster(partition, init, retries - 1)

          case EmptyResponse(_, _, p) =>
            assert(p == partition)
            Thread.sleep(PeerAgent.RETRY_SLEEP_TIME)
            updateMaster(partition, init, zkRetriesAmount)

          case DeleteMasterResponse(_, _, p) =>
            assert(p == partition)
            val newMaster = electPartitionMaster(partition)
            if (PeerAgent.logger.isDebugEnabled)
            {
              PeerAgent.logger.debug(s"[UPDATER] Finish updating master with init: {$init} on agent: {$myInetAddress} on stream: {$streamName}, partition: {$partition} with retry=$retries; revoted master: {$newMaster}.")
            }
            agentsStateManager.putPartitionMasterLocally(partition, newMaster)
        }
      } else {
        transport.pingRequest(master.agentAddress, partition) match {
          case null =>
            if (retries == 0)
              throw new IllegalStateException(s"Agent didn't respond to me.")
            //assume that if master is not responded it will be deleted by zk
            Thread.sleep(PeerAgent.RETRY_SLEEP_TIME)
            updateMaster(partition, init, retries - 1)

          case EmptyResponse(_, _, p) =>
            assert(p == partition)
            Thread.sleep(PeerAgent.RETRY_SLEEP_TIME)
            updateMaster(partition, init, zkRetriesAmount)

          case PingResponse(_, _, p) =>
            assert(p == partition)
            if (PeerAgent.logger.isDebugEnabled)
            {
              PeerAgent.logger.debug(s"[UPDATER] Finish updating master with init: {$init} on agent: {$myInetAddress} on stream: {$streamName}, partition: {$partition} with retry=$retries; old master: {$master} is alive now.")
            }
            agentsStateManager.putPartitionMasterLocally(partition, master.agentAddress)
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
              PeerAgent.logger.error(s"Agent ${agent.getProducer.name} ${myInetAddress} - Zk connection Lost. Immediately shutdown.")
              System.exit(1)
            }
            Thread.sleep(PeerAgent.RETRY_SLEEP_TIME)
          }
        } catch {
          case e: Exception =>
            PeerAgent.logger.error(s"Agent ${agent.getProducer.name} ${myInetAddress} - Zk connection Lost. Immediately shutdown.")
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
    * @return Transaction UUID
    */
  def generateNewTransaction(partition: Int): UUID = {
    LockUtil.withLockOrDieDo[UUID](externalAccessLock, (100, TimeUnit.SECONDS), Some(PeerAgent.logger), () => {
      val master = agentsStateManager.getPartitionMasterLocally(partition, null)
      if (PeerAgent.logger.isDebugEnabled)
      {
        PeerAgent.logger.debug(s"[GETTXN] Start retrieve txn for agent with address: {$myInetAddress}, stream: {$streamName}, partition: {$partition} from [MASTER: {$master}].")
      }

      val res =
        if (master != null) {
          val txnResponse = transport.transactionRequest(master, partition)
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
              if (PeerAgent.logger.isDebugEnabled)
              {
                PeerAgent.logger.debug(s"[GETTXN] Finish retrieve txn for agent with address: {$myInetAddress}, stream: {$streamName}, partition: {$partition} with timeuuid: {${uuid.timestamp()}} from [MASTER: {$master}]s")
              }
              uuid
          }
        } else {
          updateMaster(partition, init = false)
          generateNewTransaction(partition)
        }
      res
    })
  }

  def notifyMaterialize(msg: TransactionStateMessage, to: String): Unit = {
    if (PeerAgent.logger.isDebugEnabled)
    {
      PeerAgent.logger.debug(s"[MATERIALIZE] Send materialize request address\nMe: {$myInetAddress}\nTXN owner: ${to}\nStream: ${streamName}\npartition: ${msg.partition}\nTXN: ${msg.txnUuid}")
    }
    transport.materializeRequest(to, msg)
  }

  //TODO remove after complex testing
  def publish(msg: TransactionStateMessage): Unit = {
    LockUtil.withLockOrDieDo[Unit](externalAccessLock, (100, TimeUnit.SECONDS), Some(PeerAgent.logger), () => {
      val master = agentsStateManager.getPartitionMasterLocally(msg.partition, null)
      if (PeerAgent.logger.isDebugEnabled)
        PeerAgent.logger.debug(s"[PUBLISH] SEND PTM:{$msg} to [MASTER:{$master}] from agent:{$myInetAddress}," +
          s"stream:{$streamName}")
      if (master != null) {
        transport.publishRequest(master, msg)
      } else {
        updateMaster(msg.partition, init = false)
        publish(msg)
      }
    })
  }

  /**
    * Stop this agent
    */
  def stop() = {
    PeerAgent.logger.info(s"P2PAgent of ${producer.name} is shutting down.")
    isRunning.set(false)
    partitionWeightDistributionThread.join()
    zkConnectionValidator.join()
    //to avoid infinite polling block
    executorGraphs.foreach(g => g._2.shutdown())
    agentsStateManager.shutdown()
    transport.stop()
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
        case _: PublishRequest =>         executorGraphs(execNo).submitToPublish(task)
        case _: NewTransactionRequest =>  executorGraphs(execNo).submitToNewTransaction(task)
        case _: MaterializeRequest =>     executorGraphs(execNo).submitToMaterialize(task)
        case _ =>                         executorGraphs(execNo).submitToGeneral(task)
      }

    } catch {
      case e: ProtocolMessageSerializerException =>
        PeerAgent.logger.warn(s"Message '${rawMessage}' cannot be deserialized. Exception is: ${e.getMessage}")
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
    executorGraphs(execNum).submitToPublish(task)
  }

  /**
    * public method which allows to submit delayed task for execution
    *
    * @param task
    * @param partition
    */
  def submitPipelinedTaskToMaterializeExecutor(partition: Int, task: () => Unit) = {
    val execNum = partitionsToExecutors(partition)
    executorGraphs(execNum).submitToMaterialize(task)
  }

  /**
    * public method which allows to submit delayed task for execution
    *
    * @param task
    */
  def submitPipelinedTaskToCassandraExecutor(partition: Int, task: () => Unit) = {
    val execNum = partitionsToExecutors(partition)
    executorGraphs(execNum).submitToCassandra(task)
  }

  /**
    * public method which allows to submit delayed task for execution
    *
    * @param task
    * @param partition
    */
  def submitPipelinedTaskToNewTxnExecutors(partition: Int, task: () => Unit) = {
    val execNum = partitionsToExecutors(partition)
    executorGraphs(execNum).submitToNewTransaction(task)
  }

  def getCassandraAsyncExecutor(partition: Int) =
    executorGraphs(partitionsToExecutors(partition)).getCassandra()
}