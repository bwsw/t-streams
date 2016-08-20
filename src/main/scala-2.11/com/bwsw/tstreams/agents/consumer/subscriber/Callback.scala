package com.bwsw.tstreams.agents.consumer.subscriber


/**
  * Trait to implement to handle incoming messages
  */
trait Callback[T] {
  /**
    * Callback which is called on every closed transaction
    *
    * @param partition       partition of the incoming transaction
    * @param transactionUuid time uuid of the incoming transaction
    * @param subscriber      Subscriber ref
    */
  def onEvent(subscriber: SubscribingConsumer[T], partition: Int, transactionUuid: java.util.UUID): Unit
}
