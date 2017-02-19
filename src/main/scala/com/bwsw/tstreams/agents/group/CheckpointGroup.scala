package com.bwsw.tstreams.agents.group

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.agents.consumer.RPCConsumerTransaction
import com.bwsw.tstreams.agents.producer.RPCProducerTransaction
import com.bwsw.tstreams.common.{FirstFailLockableTaskExecutor, GeneralOptions, LockUtil, StorageClient}
import org.slf4j.LoggerFactory
import transactionService.rpc.TransactionStates

import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.duration._


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
  private val lockTimeout = (CheckpointGroup.LOCK_TIMEOUT_SECONDS, TimeUnit.SECONDS)
  private val lock = new ReentrantLock()
  private val executorPool = new FirstFailLockableTaskExecutor("CheckpointGroup-Workers", executors)
  private val isStopped = new AtomicBoolean(false)

  /**
    * Add new agent in group
    *
    * @param agent Agent ref
    */
  def add(agent: GroupParticipant): Unit = {
    LockUtil.withLockOrDieDo[Unit](lock, lockTimeout, Some(CheckpointGroup.logger), () => {
      if (isStopped.get)
        throw new IllegalStateException("Group is stopped. No longer operations are possible.")
      if (agents.contains(agent.getAgentName)) {
        throw new IllegalArgumentException(s"Agent with specified name ${agent.getAgentName} is already in the group. Names of added agents must be unique.")
      }
      agents += ((agent.getAgentName, agent))
    })
  }

  /**
    * clears group
    */
  def clear(): Unit = {
    LockUtil.withLockOrDieDo[Unit](lock, lockTimeout, Some(CheckpointGroup.logger), () => {
      if (isStopped.get)
        throw new IllegalStateException("Group is stopped. No longer operations are possible.")
      agents.clear()
    })
  }

  /**
    * Remove agent from group
    *
    * @param name Agent name
    */
  def remove(name: String): Unit = {
    LockUtil.withLockOrDieDo[Unit](lock, lockTimeout, Some(CheckpointGroup.logger), () => {
      if (isStopped.get)
        throw new IllegalStateException("Group is stopped. No longer operations are possible.")
      if (!agents.contains(name)) {
        throw new IllegalArgumentException(s"Agent with specified name $name is not in the group.")
      }
      agents.remove(name)
    })
  }

  /**
    * Checks if an agent with the name is already inside
    */
  def exists(name: String): Boolean = {
    LockUtil.withLockOrDieDo[Boolean](lock, lockTimeout, Some(CheckpointGroup.logger), () => {
      if (isStopped.get)
        throw new IllegalStateException("Group is stopped. No longer operations are possible.")
      agents.contains(name)
    })
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
        consumerRequests.append(new RPCConsumerTransaction(streamName, consumerName, partition, offset))

      case ProducerCheckpointInfo(_, _, _, streamName, partition, transaction, totalCnt, ttl) =>
        producerRequests.append(new RPCProducerTransaction(streamName, partition, transaction, ttl, TransactionStates.Checkpointed, totalCnt))
    }

    Await.result(storageClient.client.putTransactions(producerRequests, consumerRequests), 1.minute)
  }

  /**
    * Commit all agents state
    */
  def checkpoint(): Unit = {
    if (isStopped.get)
      throw new IllegalStateException("Group is stopped. No longer operations are possible.")

    LockUtil.lockOrDie(lock, lockTimeout, Some(CheckpointGroup.logger))

    agents.foreach { case (name, agent) =>
      if (agent.getThreadLock() != null)
        LockUtil.lockOrDie(agent.getThreadLock(), lockTimeout, Some(CheckpointGroup.logger))
    }

    agents.foreach { case (name, agent) => if (agent.isInstanceOf[SendingAgent]) agent.asInstanceOf[SendingAgent].finalizeDataSend() }

    try {

      // receive from all agents their checkpoint information
      val checkpointStateInfo: List[CheckpointInfo] = agents.map { case (name, agent) =>
        agent.getCheckpointInfoAndClear()
      }.reduceRight((l1, l2) => l1 ++ l2)

      //assume all agents use the same metadata entity
      doGroupCheckpoint(agents.head._2.getStorageClient, checkpointStateInfo)

      // do publish post events for all producers
      publishCheckpointEventForAllProducers(checkpointStateInfo)

    }
    finally {
      // unlock all agents
      agents.foreach { case (name, agent) =>
        if (agent.getThreadLock() != null)
          agent.getThreadLock().unlock()
      }

      lock.unlock()
    }

  }


  private def publishCheckpointEventForAllProducers(producers: List[CheckpointInfo]) = {
    producers foreach {
      case ProducerCheckpointInfo(_, agent, checkpointEvent, _, _, _, _, _) =>
        executorPool.submit("<CheckpointEvent>", new Runnable {
          override def run(): Unit = agent.publish(checkpointEvent)
        })
      case _ =>
    }
  }

  /**
    * Stop group when it's no longer required
    */
  def stop(): Unit = {
    if (isStopped.getAndSet(true))
      throw new IllegalStateException("Group is stopped. No longer operations are possible.")
    executorPool.shutdownOrDie(CheckpointGroup.SHUTDOWN_WAIT_MAX_SECONDS, TimeUnit.SECONDS)
    clear()
  }

}

