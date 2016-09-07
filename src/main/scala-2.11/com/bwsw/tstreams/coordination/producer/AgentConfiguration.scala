package com.bwsw.tstreams.coordination.producer

/**
  * Agent representation of every agent in [/producers/agents/{agent}]
  *
  * @param agentAddress
  * @param penalty  Penalty for agent (if he want to have low priority to be master)
  * @param priority Amount of partitions where this agent is master
  */
case class AgentConfiguration(agentAddress: String, var priority: Int, penalty: Int, uniqueAgentID: Int) {
  override def toString(): String =
    s"AgentSettings(agentAddress='${agentAddress}',priority='${priority}', penalty='${penalty}', uniqueAgentID='${uniqueAgentID}')"
}


/**
  *
  * @param agentAddress
  * @param uniqueAgentId
  */
case class MasterConfiguration(agentAddress : String, uniqueAgentId : Int)


