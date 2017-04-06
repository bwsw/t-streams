package com.bwsw.tstreams.generator


/**
  * Trait for producer/consumer transaction unique ID generating
  */
trait ITransactionGenerator {
  def getTransaction(): Long

  def getTransaction(timestamp: Long): Long
}
