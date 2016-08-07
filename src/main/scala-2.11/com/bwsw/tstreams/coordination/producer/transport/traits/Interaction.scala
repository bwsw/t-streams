package com.bwsw.tstreams.coordination.producer.transport.traits

import java.util.UUID

import com.bwsw.tstreams.coordination.producer.p2p.PeerToPeerAgent

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
  val masterP2PAgent: PeerToPeerAgent
}
