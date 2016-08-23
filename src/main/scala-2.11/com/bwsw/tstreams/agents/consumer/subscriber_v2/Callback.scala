package com.bwsw.tstreams.agents.consumer.subscriber_v2

import com.bwsw.tstreams.agents.consumer.TransactionOperator

/**
  * Trait to implement to handle incoming messages
  */
trait Callback[T] {
  /**
    * Callback which is called on every closed transaction
    *
    * @param partition       partition of the incoming transaction
    * @param uuid time uuid of the incoming transaction
    * @param consumer        associated Consumer
    */
  def onEvent(consumer: TransactionOperator[T],
              partition: Int,
              uuid: java.util.UUID,
              count: Int): Unit
}
