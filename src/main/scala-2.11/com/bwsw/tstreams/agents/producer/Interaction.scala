package com.bwsw.tstreams.agents.producer

import java.util.UUID

import com.bwsw.tstreams.coordination.producer.PeerAgent

/**
  * Trait for producers
  */
trait Interaction {
  /**
    * Method to implement for concrete producer
    * Need only if this producer is master
    *
    * @return UUID
    */
  def openTxnLocal(txnUUID: UUID, partition: Int, onComplete: () => Unit): Unit

  /**
    * Agent for producer to provide producers communication
    */
  val p2pAgent: PeerAgent
}
