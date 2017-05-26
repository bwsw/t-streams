package com.bwsw.tstreams.agents.group

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreams.agents.consumer.RPCConsumerTransaction
import com.bwsw.tstreams.agents.producer.{Producer, RPCProducerTransaction}
import com.bwsw.tstreams.common.{FirstFailLockableTaskExecutor, GeneralOptions}
import com.bwsw.tstreams.storage.StorageClient
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._


object CheckpointGroup {
  var SHUTDOWN_WAIT_MAX_SECONDS = GeneralOptions.SHUTDOWN_WAIT_MAX_SECONDS
  val logger = LoggerFactory.getLogger(this.getClass)
}

/**
  * Base class to creating agent group
  */
class CheckpointGroup private[tstreams] (val executors: Int = 1) {
  /**
    * Group of agents (stateInfoList/consumer)
    */
  private val agents = scala.collection.mutable.Map[String, GroupParticipant]()
  private lazy val executorPool = new FirstFailLockableTaskExecutor("CheckpointGroup-Workers", executors)
  private val isStopped = new AtomicBoolean(false)
  private val log = CheckpointGroup.logger


  /**
    * Add new agent in group
    *
    * @param agent Agent ref
    */
  def add(agent: GroupParticipant): CheckpointGroup = this.synchronized {
    if (isStopped.get)
      throw new IllegalStateException("Group is stopped. No longer operations are possible.")
    if (agents.contains(agent.getAgentName)) {
      throw new IllegalArgumentException(s"Agent with specified name ${agent.getAgentName} is already in the group. Names of added agents must be unique.")
    }
    agents += ((agent.getAgentName, agent))
    this
  }

  /**
    * clears group
    */
  def clear(): CheckpointGroup = this.synchronized {
    if (isStopped.get)
      throw new IllegalStateException("Group is stopped. No longer operations are possible.")
    agents.clear()
    this
  }

  /**
    * Remove agent from group
    *
    * @param name Agent name
    */
  def remove(name: String): CheckpointGroup = this.synchronized {
    if (isStopped.get)
      throw new IllegalStateException("Group is stopped. No longer operations are possible.")
    if (!agents.contains(name)) {
      throw new IllegalArgumentException(s"Agent with specified name $name is not in the group.")
    }
    agents.remove(name)
    this
  }

  /**
    * Checks if an agent with the name is already inside
    */
  def exists(name: String): Boolean = this.synchronized {
    if (isStopped.get)
      throw new IllegalStateException("Group is stopped. No longer operations are possible.")
    agents.contains(name)
  }

  private def checkUpdateFailure(producers: List[StateInfo]) = {
    val availList = producers.map {
      case ProducerTransactionStateInfo(_, agent, _, _, _, _, _, _) => agent.getProducer.checkUpdateFailure()
      case _ => 1.minute
    }
    availList.sorted.head
  }

  /**
    * Group agents commit
    *
    * @param checkpointRequests Info to commit
    *                           (used only for consumers now; stateInfoList is not atomic)
    */
  private def doGroupCheckpoint(storageClient: StorageClient, checkpointRequests: List[StateInfo]): Unit = {
    val producerRequests = ListBuffer[RPCProducerTransaction]()
    val consumerRequests = ListBuffer[RPCConsumerTransaction]()

    checkpointRequests foreach {
      case ConsumerStateInfo(consumerName, streamName, partition, offset) =>
        consumerRequests.append(new RPCConsumerTransaction(consumerName, streamName, partition, offset))

      case ProducerTransactionStateInfo(_, _, _, streamName, partition, transaction, totalCnt, ttl) =>
        producerRequests.append(new RPCProducerTransaction(streamName, partition, transaction, TransactionStates.Checkpointed, totalCnt, ttl))
    }

    //check stateInfoList update timeout
    val availTime = checkUpdateFailure(checkpointRequests)
    storageClient.putTransactions(producerRequests, consumerRequests, availTime)
  }

  private def doGroupCancel(storageClient: StorageClient, checkpointRequests: List[StateInfo]): Unit = {
    val producerRequests = ListBuffer[RPCProducerTransaction]()
    checkpointRequests foreach {
      case ConsumerStateInfo(consumerName, streamName, partition, offset) =>
      case ProducerTransactionStateInfo(_, _, _, streamName, partition, transaction, totalCnt, ttl) =>
        producerRequests.append(new RPCProducerTransaction(streamName, partition, transaction, TransactionStates.Cancel, totalCnt, ttl))
    }

    //check stateInfoList update timeout
    val availTime = checkUpdateFailure(checkpointRequests)
    storageClient.putTransactions(producerRequests, Nil, availTime)
  }

  /**
    * Commit all agents state
    */
  def checkpoint(): Unit = this.synchronized {
    if (isStopped.get)
      throw new IllegalStateException("Group is stopped. No longer operations are possible.")

    if(agents.isEmpty)
      return

    if(log.isDebugEnabled())
      log.debug("Complete send data to storage server for all participants")

    agents.foreach { case (name, agent) => if (agent.isInstanceOf[SendingAgent]) agent.asInstanceOf[SendingAgent].finalizeDataSend() }

    if(log.isDebugEnabled())
      log.debug("Gather checkpoint information from participants")

    // receive from all agents their checkpoint information
    val checkpointStateInfo: List[StateInfo] = agents
      .map { case (name, agent) => agent.getCheckpointInfoAndClear() }
      .reduceRight((l1, l2) => l1 ++ l2)

    if(checkpointStateInfo.isEmpty)
      return

    if(log.isDebugEnabled()) {
      log.debug(s"CheckpointGroup Info ${checkpointStateInfo}\n" + "Do group checkpoint.")
    }
    //assume all agents use the same metadata entity
    doGroupCheckpoint(agents.head._2.getStorageClient, checkpointStateInfo)

    if(log.isDebugEnabled()) log.debug("Do publish notifications")

    publishEventForAllProducers(checkpointStateInfo)

    if(log.isDebugEnabled()) log.debug("End checkpoint")
  }


  def cancel(): Unit = this.synchronized {
    if (isStopped.get)
      throw new IllegalStateException("Group is stopped. No longer operations are possible.")

    if(agents.isEmpty)
      return

    // receive from all agents their checkpoint information
    val cancelStateInfo: List[StateInfo] = agents
      .filter { case (name, agent) => agent.isInstanceOf[Producer] }
      .map { case (name, agent) => agent.asInstanceOf[Producer].getCancelInfoAndClear() }
      .reduceRight((l1, l2) => l1 ++ l2)

    if(cancelStateInfo.isEmpty)
      return

    if(log.isDebugEnabled()) {
      log.debug(s"CheckpointGroup Info ${cancelStateInfo}\n" + "Do group cancel.")
    }

    //assume all agents use the same metadata entity
    doGroupCancel(agents.head._2.getStorageClient, cancelStateInfo)

    if(log.isDebugEnabled()) log.debug("Do publish notifications")

    publishEventForAllProducers(cancelStateInfo)

    if(log.isDebugEnabled()) log.debug("End checkpoint")

  }


  private def publishEventForAllProducers(stateInfoList: List[StateInfo]) = {
    stateInfoList foreach {
      case ProducerTransactionStateInfo(_, agent, stateEvent, _, _, _, _, _) =>
        executorPool.submit("<Event>", () => agent.getProducer.publish(stateEvent, agent.getProducer.stream.client.authenticationKey), None)
      case _ =>
    }
  }

  /**
    * Stop group when it's no longer required
    */
  def stop(): Unit = this.synchronized {
    clear()
    if (isStopped.getAndSet(true))
      throw new IllegalStateException("Group is stopped. No longer operations are possible.")
    executorPool.shutdownOrDie(CheckpointGroup.SHUTDOWN_WAIT_MAX_SECONDS, TimeUnit.SECONDS)
  }

}

