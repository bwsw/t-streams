package com.bwsw.tstreams.agents.group

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.common.{FirstFailLockableTaskExecutor, LockUtil}
import com.bwsw.tstreams.metadata.{RequestsRepository, MetadataStorage, TransactionDatabase}
import com.datastax.driver.core.BatchStatement
import org.slf4j.LoggerFactory

/**
  * Base class to creating agent group
  */
class CheckpointGroup(val executors: Int = 1) {
  /**
    * Group of agents (producers/consumer)
    */
  private var agents = scala.collection.mutable.Map[String, GroupParticipant]()
  private val lock = new ReentrantLock()
  private val lockTimeout = (20, TimeUnit.SECONDS)
  private val executorPool = new FirstFailLockableTaskExecutor("CheckpointGroup-Workers", executors)
  private val isStopped = new AtomicBoolean(false)

  /**
    * MetadataStorage logger for logging
    */
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * Validate that all agents has the same metadata storage
    */
  private def checkIfAgentsUseSameMetadataStorage() = {
    var set = Set[String]()
    agents.map(x => x._2.getMetadataRef().id).foreach(id => set += id)
    if (set.size != 1)
      throw new IllegalStateException("All agents in a group must use the same metadata storage.")
  }

  /**
    * Add new agent in group
    *
    * @param agent Agent ref
    */
  def add(agent: GroupParticipant): Unit = {
    LockUtil.withLockOrDieDo[Unit](lock, lockTimeout, Some(logger), () => {
      if (isStopped.get)
        throw new IllegalStateException("Group is stopped. No longer operations are possible.")
      if (agents.contains(agent.getAgentName)) {
        throw new IllegalArgumentException(s"Agent with specified name ${agent.getAgentName} is already in the group. Names of added agents must be unique.")
      }
      agents += ((agent.getAgentName, agent))
      checkIfAgentsUseSameMetadataStorage()
    })
  }

  /**
    * clears group
    */
  def clear(): Unit = {
    LockUtil.withLockOrDieDo[Unit](lock, lockTimeout, Some(logger), () => {
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
    LockUtil.withLockOrDieDo[Unit](lock, lockTimeout, Some(logger), () => {
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
    LockUtil.withLockOrDieDo[Boolean](lock, lockTimeout, Some(logger), () => {
      if (isStopped.get)
        throw new IllegalStateException("Group is stopped. No longer operations are possible.")
      agents.contains(name)
    })
  }

  /**
    * Group agents commit
    *
    * @param info Info to commit
    *             (used only for consumers now; producers is not atomic)
    */
  private def doGroupCheckpoint(metadata: MetadataStorage, info: List[CheckpointInfo]): Unit = {
    val batchStatement = new BatchStatement()
    val session = metadata.getSession()
    val requests = RequestsRepository.getStatements(session)
    info foreach {
      case ConsumerCheckpointInfo(name, stream, partition, offset) =>
        batchStatement.add(requests.consumerCheckpointStatement.bind(name, stream, new Integer(partition), new java.lang.Long(offset)))

      case ProducerCheckpointInfo(_, _, _, _, streamName, partition, transaction, totalCnt, ttl) =>

        val interval = new java.lang.Long(TransactionDatabase.getAggregationInterval(transaction))

        batchStatement.add(requests.commitLogPutStatement.bind(streamName, new Integer(partition),
          interval , new java.lang.Long(transaction), new Integer(totalCnt), new Integer(ttl)))
    }
    metadata.getSession().execute(batchStatement)
  }

  /**
    * Commit all agents state
    */
  def checkpoint(): Unit = {
    if (isStopped.get)
      throw new IllegalStateException("Group is stopped. No longer operations are possible.")

    LockUtil.lockOrDie(lock, lockTimeout, Some(logger))

    agents.foreach { case (name, agent) =>
      if (agent.getThreadLock() != null)
        LockUtil.lockOrDie(agent.getThreadLock(), lockTimeout, Some(logger))
    }

    agents.foreach { case (name, agent) => if (agent.isInstanceOf[SendingAgent]) agent.asInstanceOf[SendingAgent].finalizeDataSend() }

    try {

      // receive from all agents their checkpoint information
      val checkpointStateInfo: List[CheckpointInfo] = agents.map { case (name, agent) =>
        agent.getCheckpointInfoAndClear()
      }.reduceRight((l1, l2) => l1 ++ l2)

      // do publish pre events for all producers
      publishPreCheckpointEventForAllProducers(checkpointStateInfo)

      //assume all agents use the same metadata entity
      doGroupCheckpoint(agents.head._2.getMetadataRef(), checkpointStateInfo)

      // do publish post events for all producers
      publishPostCheckpointEventForAllProducers(checkpointStateInfo)

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

  private def publishPreCheckpointEventForAllProducers(producers: List[CheckpointInfo]) = {
    val l = new CountDownLatch(producers.size)
    producers foreach {
      case ProducerCheckpointInfo(transaction, agent, preCheckpointEvent, _, _, _, _, _, _) =>
        executorPool.submit("<PreCheckpointEvent>", new Runnable {
          override def run(): Unit = {
            agent.publish(preCheckpointEvent)

            if(logger.isDebugEnabled)
              logger.debug("PRE event sent for " + transaction.getTransactionID.toString)

            l.countDown()
          }
        })
      case _ => l.countDown()
    }
    l.await()
  }

  private def publishPostCheckpointEventForAllProducers(producers: List[CheckpointInfo]) = {
    producers foreach {
      case ProducerCheckpointInfo(_, agent, _, postCheckpointEvent, _, _, _, _, _) =>
        executorPool.submit("<PostCheckpointEvent>", new Runnable {
          override def run(): Unit = agent.publish(postCheckpointEvent)
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
    executorPool.shutdownOrDie(100, TimeUnit.SECONDS)
    clear()
  }

}
