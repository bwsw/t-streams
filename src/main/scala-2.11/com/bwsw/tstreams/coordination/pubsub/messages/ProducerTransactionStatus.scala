package com.bwsw.tstreams.coordination.pubsub.messages

/**
 * Status for producer topic messages
 */
object ProducerTransactionStatus extends Enumeration {
  type ProducerTransactionStatus = Value

  /**
   * If transaction is opened
   */
  val opened = Value

  /**
   * If transaction is closed
   */
  val closed = Value

  /**
   * If transaction is cancelled
   */
  val cancelled = Value

  /**
   * If transaction is updated
   */
  val updated = Value

}