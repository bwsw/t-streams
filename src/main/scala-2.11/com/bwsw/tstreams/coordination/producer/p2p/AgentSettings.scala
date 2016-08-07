package com.bwsw.tstreams.coordination.producer.p2p

/**
  * Agent representation of every agent in [/producers/agents/{agent}]
  *
  * @param id       Agent id
  * @param penalty  Penalty for agent (if he want to have low priority to be master)
  * @param priority Amount of partitions where this agent is master
  */
case class AgentSettings(id: String, var priority: Int, penalty: Int)


