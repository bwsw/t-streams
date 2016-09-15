package com.bwsw.tstreams.coordination.messages.state

/**
  * Status for producer topic messages
  */
object TransactionStatus extends Enumeration {
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

  /**
    * Transaction to be materialized
    */
  val materialize = Value

  /**
    * Transaction is invalid
    */
  val invalid = Value

}