package com.bwsw.tstreams.agents.consumer.subscriber

import com.bwsw.tstreams.agents.consumer.{ConsumerTransaction, TransactionOperator}
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates

/**
  * Trait to implement to handle incoming messages
  */
trait Callback {
  /**
    * Callback which is called on every closed transaction
    *
    * @param consumer    associated Consumer
    * @param transaction the transaction which currently has delivered
    */
  def onTransaction(consumer: TransactionOperator,
                    transaction: ConsumerTransaction): Unit

  /**
    *
    * @param consumer      consumer object which is associated with the transaction
    * @param partition     partition on which the transaction is
    * @param transactionID transaction ID
    * @param count         amount of data items inside of the transaction
    */
  def onTransactionCall(consumer: TransactionOperator,
                        partition: Int,
                        transactionID: Long,
                        count: Int) = {
    consumer.setStreamPartitionOffset(partition, transactionID)
    consumer.buildTransactionObject(partition, transactionID, TransactionStates.Checkpointed, count)
      .foreach(transaction => onTransaction(consumer, transaction = transaction))
  }
}
