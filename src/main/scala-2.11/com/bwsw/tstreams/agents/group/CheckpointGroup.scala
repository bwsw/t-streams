package com.bwsw.tstreams.agents.group

import java.util.concurrent.locks.ReentrantLock

/**
  * Base class to creating agent group
  */
class CheckpointGroup() {
  /**
    * Group of agents (producers/consumer)
    */
  private var agents = scala.collection.mutable.Map[String, Agent]()
  private val lock = new ReentrantLock()

  /**
    * Validate that all agents has the same metadata storage
    */
  private def validateAgents() = {
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
  def add(agent: Agent): Unit = {
    lock.lock()
    if (agents.contains(agent.getAgentName)) {
      lock.unlock()
      throw new IllegalArgumentException(s"Agent with specified name ${agent.getAgentName} is already in the group. Names of added agents must be unique.")
    }
    agents += ((agent.getAgentName, agent))
    validateAgents()
    lock.unlock()
  }

  /**
    * clears group
    */
  def clear(): Unit = {
    lock.lock()
    agents.clear()
    lock.unlock()
  }

  /**
    * Remove agent from group
    *
    * @param name Agent name
    */
  def remove(name: String): Unit = {
    lock.lock()
    if (!agents.contains(name)) {
      lock.unlock()
      throw new IllegalArgumentException(s"Agent with specified name ${name} is not in the group.")
    }
    agents.remove(name)
    lock.unlock()
  }

  /**
    * Checks if an agent with the name is already inside
    */
  def exists(name: String): Boolean = {
    lock.lock()
    val r = agents.contains(name)
    lock.unlock()
    r
  }

  /**
    * Commit all agents state
    */
  def checkpoint(): Unit = {
    // lock from race
    lock.lock()

    // lock all agents
    agents.foreach { case (name, agent) =>
      agent.getThreadLock().lock()
    }

    var exc: Exception = null

    try {

      // receive from all agents their checkpoint information
      val checkpointStateInfo: List[CheckpointInfo] = agents.map { case (name, agent) =>
        agent.getCheckpointInfoAndClear()
      }.reduceRight((l1, l2) => l1 ++ l2)

      // do publish pre events for all producers
      publishPreCheckpointEventForAllProducers(checkpointStateInfo)

      //assume all agents use the same metadata entity
      agents.head._2.getMetadataRef().groupCheckpointEntity.groupCheckpoint(checkpointStateInfo)

      // do publish post events for all producers
      publishPostCheckpointEventForAllProducers(checkpointStateInfo)

    }
    catch {
      case e: Exception =>
        exc = e
    }
    // unlock all agents
    agents.foreach { case (name, agent) =>
      agent.getThreadLock().unlock()
    }
    lock.unlock()
    throw exc
  }

  private def publishPreCheckpointEventForAllProducers(info: List[CheckpointInfo]) = {
    info foreach {
      case ProducerCheckpointInfo(_, agent, preCheckpointEvent, _, _, _, _, _, _) =>
        agent.publish(preCheckpointEvent)
      case _ =>
    }
  }

  private def publishPostCheckpointEventForAllProducers(info: List[CheckpointInfo]) = {
    info foreach {
      case ProducerCheckpointInfo(_, agent, _, finalCheckpointEvent, _, _, _, _, _) =>
        agent.publish(finalCheckpointEvent)
      case _ =>
    }
  }
}
