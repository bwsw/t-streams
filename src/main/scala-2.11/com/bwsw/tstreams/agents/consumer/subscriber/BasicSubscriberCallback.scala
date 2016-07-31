package com.bwsw.tstreams.agents.consumer.subscriber


/**
  * Trait to implement to handle incoming messages
  */
trait BasicSubscriberCallback[USERTYPE] {
  /**
    * Callback which is called on every closed transaction
    *
    * @param partition       partition of the incoming transaction
    * @param transactionUuid time uuid of the incoming transaction
    * @param subscriber      Subscriber ref
    */
  def onEvent(subscriber: BasicSubscribingConsumer[USERTYPE], partition: Int, transactionUuid: java.util.UUID): Unit

  /**
    * Frequency of handling incoming transactions in milliseconds
    *
    * If handling thread meet a opened transaction it will sleep [[pollingFrequency]]]
    * else he will consume all closed transactions
    */
  val pollingFrequency: Int
}
