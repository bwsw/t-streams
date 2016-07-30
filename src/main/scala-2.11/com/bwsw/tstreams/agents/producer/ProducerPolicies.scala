package com.bwsw.tstreams.agents.producer

/**
  * Producer policies for newTransaction method
  */
object ProducerPolicies extends Enumeration {
  type ProducerPolicy = Value

  /**
    * If previous transaction was opened it will be closed
    */
  val checkpointIfOpened = Value

  /**
    * If previous transaction was opened it will be canceled
    */
  val cancelIfOpened = Value

  /**
    * If previous transaction was opened exception will be thrown
    */
  val errorIfOpened = Value
}
