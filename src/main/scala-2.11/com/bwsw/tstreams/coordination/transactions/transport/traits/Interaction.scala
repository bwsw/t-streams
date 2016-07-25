package com.bwsw.tstreams.coordination.transactions.transport.traits

import java.util.UUID

import com.bwsw.tstreams.coordination.transactions.peertopeer.PeerToPeerAgent

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
  def openTxnLocal(txnUUID : UUID, partition : Int, onComplete: () => Unit) : Unit

  /**
   * Agent for producer to provide producers communication
   */
  val master_p2p_agent : PeerToPeerAgent
}
