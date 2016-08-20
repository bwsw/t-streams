package com.bwsw.tstreams.agents.consumer.subscriber_v2

import com.bwsw.tstreams.agents.consumer.Consumer

/**
  * Trait to implement to handle incoming messages
  */
trait Callback[T] {
  /**
    * Callback which is called on every closed transaction
    *
    * @param partition       partition of the incoming transaction
    * @param transactionUuid time uuid of the incoming transaction
    * @param consumer        associated Consumer
    */
  def onEvent(consumer: Consumer[T],
              partition: Int,
              transactionUuid: java.util.UUID,
              count: Int): Unit = {
  }
}
