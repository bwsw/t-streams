package com.bwsw.tstreams.agents.producer

/**
  * Producer policies for newTransaction method
  */
object NewTransactionProducerPolicy extends Enumeration {
  type ProducerPolicy = Value

  /**
    * If previous transaction was opened it will be closed
    */
  val CheckpointIfOpened = Value

  /**
    * If previous transaction was opened it will be canceled
    */
  val CancelIfOpened = Value

  /**
    * If previous transaction was opened exception will be thrown
    */
  val ErrorIfOpened = Value
}
