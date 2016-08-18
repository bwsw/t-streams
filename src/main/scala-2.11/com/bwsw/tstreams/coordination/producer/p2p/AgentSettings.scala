package com.bwsw.tstreams.coordination.producer.p2p

/**
  * Agent representation of every agent in [/producers/agents/{agent}]
  *
  * @param agentAddress
  * @param penalty  Penalty for agent (if he want to have low priority to be master)
  * @param priority Amount of partitions where this agent is master
  */
case class AgentSettings(agentAddress: String, var priority: Int, penalty: Int) {
  override def toString(): String =
    s"AgentSettings(agentAddress='${agentAddress}',priority='${priority}', penalty='${penalty}')"
}


/**
  *
  * @param agentAddress
  * @param uniqueAgentId
  */
case class MasterSettings(agentAddress : String, uniqueAgentId : Int)


