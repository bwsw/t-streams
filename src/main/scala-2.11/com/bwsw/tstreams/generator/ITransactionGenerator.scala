package com.bwsw.tstreams.generator


/**
  * Trait for producer/consumer transaction unique ID generating
  */
trait ITransactionGenerator {
  /**
    * @return ID
    */
  def getTransaction(): Long

  /**
    * @return ID based on timestamp
    */
  def getTransaction(timestamp: Long): Long
}
