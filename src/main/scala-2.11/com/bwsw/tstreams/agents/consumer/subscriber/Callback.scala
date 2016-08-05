package com.bwsw.tstreams.agents.consumer.subscriber


/**
  * Trait to implement to handle incoming messages
  */
trait Callback[USERTYPE] {
  /**
    * Callback which is called on every closed transaction
    *
    * @param partition       partition of the incoming transaction
    * @param transactionUuid time uuid of the incoming transaction
    * @param subscriber      Subscriber ref
    */
  def onEvent(subscriber: SubscribingConsumer[USERTYPE], partition: Int, transactionUuid: java.util.UUID): Unit
}