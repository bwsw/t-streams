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
    * If transaction in pre checkpointed
    */
  val preCheckpoint = Value

  /**
    * If transaction is finally checkpointed
    */
  val postCheckpoint = Value

  /**
    * If transaction is cancelled
    */
  val cancel = Value

  /**
    * If transaction is updated
    */
  val update = Value

}