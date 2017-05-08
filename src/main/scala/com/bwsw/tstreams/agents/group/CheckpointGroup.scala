package com.bwsw.tstreams.agents.group

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreams.agents.consumer.RPCConsumerTransaction
import com.bwsw.tstreams.agents.producer.RPCProducerTransaction
import com.bwsw.tstreams.common.{FirstFailLockableTaskExecutor, GeneralOptions}
import com.bwsw.tstreams.storage.StorageClient
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer


object CheckpointGroup {
  var SHUTDOWN_WAIT_MAX_SECONDS = GeneralOptions.SHUTDOWN_WAIT_MAX_SECONDS
  var LOCK_TIMEOUT_SECONDS = 20
  val logger = LoggerFactory.getLogger(this.getClass)
}

/**
  * Base class to creating agent group
  */
class CheckpointGroup(val executors: Int = 1) {
  /**
    * Group of agents (producers/consumer)
    */
  private var agents = scala.collection.mutable.Map[String, GroupParticipant]()
  private val executorPool = new FirstFailLockableTaskExecutor("CheckpointGroup-Workers", executors)
  private val isStopped = new AtomicBoolean(false)

  /**
    * Add new agent in group
    *
    * @param agent Agent ref
    */
  def add(agent: GroupParticipant): Unit = this.synchronized {
    if (isStopped.get)
      throw new IllegalStateException("Group is stopped. No longer operations are possible.")
    if (agents.contains(agent.getAgentName)) {
      throw new IllegalArgumentException(s"Agent with specified name ${agent.getAgentName} is already in the group. Names of added agents must be unique.")
    }
    agents += ((agent.getAgentName, agent))
  }

  /**
    * clears group
    */
  def clear(): Unit = this.synchronized {
    if (isStopped.get)
      throw new IllegalStateException("Group is stopped. No longer operations are possible.")
    agents.clear()
  }

  /**
    * Remove agent from group
    *
    * @param name Agent name
    */
  def remove(name: String): Unit = this.synchronized {
    if (isStopped.get)
      throw new IllegalStateException("Group is stopped. No longer operations are possible.")
    if (!agents.contains(name)) {
      throw new IllegalArgumentException(s"Agent with specified name $name is not in the group.")
    }
    agents.remove(name)
  }

  /**
    * Checks if an agent with the name is already inside
    */
  def exists(name: String): Boolean = this.synchronized {
    if (isStopped.get)
      throw new IllegalStateException("Group is stopped. No longer operations are possible.")
    agents.contains(name)
  }

  /**
    * Group agents commit
    *
    * @param checkpointRequests Info to commit
    *                           (used only for consumers now; producers is not atomic)
    */
  private def doGroupCheckpoint(storageClient: StorageClient, checkpointRequests: List[CheckpointInfo]): Unit = {
    val producerRequests = ListBuffer[RPCProducerTransaction]()
    val consumerRequests = ListBuffer[RPCConsumerTransaction]()

    checkpointRequests foreach {
      case ConsumerCheckpointInfo(consumerName, streamName, partition, offset) =>
        consumerRequests.append(new RPCConsumerTransaction(consumerName, streamName, partition, offset))

      case ProducerCheckpointInfo(_, _, _, streamName, partition, transaction, totalCnt, ttl) =>
        producerRequests.append(new RPCProducerTransaction(streamName, partition, transaction, TransactionStates.Checkpointed, totalCnt, ttl))
    }

    storageClient.putTransactions(producerRequests, consumerRequests)
  }

  /**
    * Commit all agents state
    */
  def checkpoint(): Unit = this.synchronized {
    if (isStopped.get)
      throw new IllegalStateException("Group is stopped. No longer operations are possible.")

    CheckpointGroup.logger.debug("Complete send data to storage server for all participants")
    agents.foreach { case (name, agent) => if (agent.isInstanceOf[SendingAgent]) agent.asInstanceOf[SendingAgent].finalizeDataSend() }

    CheckpointGroup.logger.debug("Gather checkpoint information from participants")
    // receive from all agents their checkpoint information
    val checkpointStateInfo: List[CheckpointInfo] = agents.map { case (name, agent) =>
      agent.getCheckpointInfoAndClear()
    }.reduceRight((l1, l2) => l1 ++ l2)

    CheckpointGroup.logger.debug(s"CheckpointGroup Info ${checkpointStateInfo}")

    CheckpointGroup.logger.debug("Do group checkpoint")
    //assume all agents use the same metadata entity
    doGroupCheckpoint(agents.head._2.getStorageClient, checkpointStateInfo)

    CheckpointGroup.logger.debug("Do publish notifications")
    // do publish post events for all producers
    publishCheckpointEventForAllProducers(checkpointStateInfo)

    CheckpointGroup.logger.debug("End checkpoint")
  }


  private def publishCheckpointEventForAllProducers(producers: List[CheckpointInfo]) = {
    producers foreach {
      case ProducerCheckpointInfo(_, agent, checkpointEvent, _, _, _, _, _) =>
        executorPool.submit("<CheckpointEvent>", () => agent.getSubscriberNotifier().publish(checkpointEvent), None)
      case _ =>
    }
  }

  /**
    * Stop group when it's no longer required
    */
  def stop(): Unit = this.synchronized {
    if (isStopped.getAndSet(true))
      throw new IllegalStateException("Group is stopped. No longer operations are possible.")
    executorPool.shutdownOrDie(CheckpointGroup.SHUTDOWN_WAIT_MAX_SECONDS, TimeUnit.SECONDS)
    clear()
  }

}

