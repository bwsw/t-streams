package com.bwsw.tstreams.agents.producer

/**
  * Producer policies for newTransaction method
  */
object NewTransactionProducerPolicy extends Enumeration {
  type ProducerPolicy = Value

  /**
    * If previous transaction was opened it will be checkpointed synchronously
    */
  val CheckpointIfOpened = Value

  /**
    * If previous transaction was opened it will be checkpointed asynchronously
    */
  val CheckpointAsyncIfOpened = Value

  /**
    * If previous transaction was opened it will be canceled synchronously
    */
  val CancelIfOpened = Value

  /**
    * If previous transaction was opened exception will be thrown
    */
  val ErrorIfOpened = Value

  /**
    * If a transaction is opened, just append it to the end of the list
    */
  val EnqueueIfOpened = Value

}
