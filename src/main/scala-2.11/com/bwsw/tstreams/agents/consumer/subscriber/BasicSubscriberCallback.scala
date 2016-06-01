package com.bwsw.tstreams.agents.consumer.subscriber


/**
 * Trait to implement to handle incoming messages
 */
trait BasicSubscriberCallback[DATATYPE, USERTYPE] {
  /**
   * Callback which is called on every closed transaction
   * @param partition partition of the incoming transaction
   * @param transactionUuid time uuid of the incoming transaction
   */
  def onEvent(subscriber : BasicSubscribingConsumer[DATATYPE, USERTYPE], partition : Int, transactionUuid : java.util.UUID) : Unit

  /**
   * Frequency of handling incoming transactions in milliseconds
   */
  val pollingFrequency : Int
}
