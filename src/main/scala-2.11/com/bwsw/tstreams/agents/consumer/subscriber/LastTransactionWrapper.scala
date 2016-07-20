package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.UUID

/**
  * Wrapper for last transaction which is used by [[com.bwsw.tstreams.agents.consumer.subscriber.SubscriberTransactionsRelay]]
  * @param initVal
  */
class LastTransactionWrapper(private var initVal : UUID) {
  /**
    *
    * @param newVal
    */
  def set(newVal : UUID) = initVal = newVal

  /**
    *
    * @return
    */
  def get() = initVal
}
