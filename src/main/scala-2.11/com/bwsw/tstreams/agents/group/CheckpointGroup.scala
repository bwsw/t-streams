package com.bwsw.tstreams.agents.group

/**
 * Base class to creating agent group
 */
class CheckpointGroup() {
  /**
   * Group of agents (producers/consumer)
   */
  private var agents = scala.collection.mutable.Map[String,Agent]()

  /**
   * Validate that all agents has the same metadata storage
   */
  private def validateAgents() = {
    var set = Set[String]()
    agents.map(x=>x._2.getMetadataRef().id).foreach(id => set += id)
    if (set.size != 1)
      throw new IllegalStateException("agents must use only one common metadata storage")
  }

  /**
   * Add new agent in group
    *
    * @param agent Agent ref
   * @param name Agent name
   */
  def add(name : String, agent : Agent) : Unit = {
    if (agents.contains(name))
      throw new IllegalArgumentException("agents with such name already exist")
    agents += ((name, agent))
    validateAgents()
  }

  /**
   * Remove agent from group
    *
    * @param name Agent name
   */
  def remove(name : String) : Unit = {
    if (!agents.contains(name))
      throw new IllegalArgumentException("agents with such name does not exist")
    agents.remove(name)
  }

  /**
   * Commit all agents state
   */
  def commit() : Unit = {
    agents.foreach{ case(name,agent) =>
      agent.getThreadLock().lock()
    }
    val totalCommitInfo: List[CheckpointInfo] = agents.map{ case(name,agent) =>
      agent.getCheckpointInfoAndClear()
    }.reduceRight((l1,l2)=>l1 ++ l2)
    publishGlobalPreCheckpointEvent(totalCommitInfo)
    stopTransactionKeepAliveUpdates(totalCommitInfo)
    //assume all agents use the same metadata entity
    agents.head._2.getMetadataRef().groupCommitEntity.groupCommit(totalCommitInfo)
    publishGlobalFinalCheckpointEvent(totalCommitInfo)
    agents.foreach{ case(name,agent) =>
      agent.getThreadLock().unlock()
    }
  }

  private def publishGlobalPreCheckpointEvent(info : List[CheckpointInfo]) = {
    info foreach {
      case ProducerCheckpointInfo(_, agent, preCheckpointEvent, _, _, _, _, _, _) =>
        agent.publish(preCheckpointEvent)
      case _ =>
    }
  }

  private def stopTransactionKeepAliveUpdates(info : List[CheckpointInfo]) = {
    info foreach {
      case ProducerCheckpointInfo(txnRef, _, _, _, _, _, _, _, _) =>
        txnRef.stopKeepAlive()
      case _ =>
    }
  }

  private def publishGlobalFinalCheckpointEvent(info : List[CheckpointInfo]) = {
    info foreach {
      case ProducerCheckpointInfo(_, agent, _, finalCheckpointEvent, _, _, _, _, _) =>
        agent.publish(finalCheckpointEvent)
      case _ =>
    }
  }
}
