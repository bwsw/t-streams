package com.bwsw.tstreams.agents.producer

/**
  * Trait for producers
  */
trait Interaction {
  /**
    * Method to implement for concrete producer
    * Need only if this producer is master
    *
    * @return ID
    */
  def openTransactionLocal(transactionID: Long, partition: Int, onComplete: () => Unit): Unit

  /**
    * Agent for producer to provide producers communication
    */
  val p2pAgent: PeerAgent
}
