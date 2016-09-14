package com.bwsw.tstreams.agents.consumer.subscriber

import com.bwsw.tstreams.agents.consumer.{Consumer, Transaction, TransactionOperator}

/**
  * Trait to implement to handle incoming messages
  */
trait Callback[T] {
  /**
    * Callback which is called on every closed transaction
    *
    * @param consumer        associated Consumer
    * @param transaction
    */
  def onEvent(consumer: TransactionOperator[T],
              transaction: Transaction[T]): Unit

  /**
    *
    * @param consumer
    * @param partition
    * @param uuid
    * @param count
    */
  def onEventCall(consumer: TransactionOperator[T],
                  partition: Int,
                  uuid: java.util.UUID,
                  count: Int) = {
    val txnOpt = consumer.buildTransactionObject(partition, uuid, count)
    txnOpt.foreach(txn => onEvent(consumer, transaction = txn))
  }
}
