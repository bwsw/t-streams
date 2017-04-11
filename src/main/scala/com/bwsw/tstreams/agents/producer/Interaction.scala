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
  private[tstreams] def openTransactionLocal(transactionID: Long, partition: Int): Unit

  /**
    * Agent for producer to provide producers communication
    */
  private[tstreams] val p2pAgent: PeerAgent
}
