package com.bwsw.tstreams.agents.producer

import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy.ProducerPolicy
import com.bwsw.tstreams.coordination.producer.PeerAgent

import scala.collection.mutable

/**
  * Created by Ivan Kudryavtsev on 28.08.16.
  */
class OpenTransactionsKeeper[T] {
  private val openTransactionsMap     = mutable.Map[Int, Transaction[T]]()

  /**
    * Allows to do smth with all not closed transactions.
    *
    * @param f
    * @tparam RV
    * @return
    */
  def forallKeysDo[RV](f: (Int, Transaction[T]) => RV): Iterable[RV] = {
    PeerAgent.logger.info("forallkeysdo")
    this.synchronized {
      openTransactionsMap.filter(kv => !kv._2.isClosed).map(kv => f(kv._1, kv._2))
    }
  }

  /**
    * Returns if transaction is in map without checking if it's not closed
    *
    * @param partition
    * @return
    */
  def getTransactionOptionNaive(partition: Int) = {
    PeerAgent.logger.info("getTransactionOptionNaive")
    this.synchronized {
      if(openTransactionsMap.contains(partition))
        Option(openTransactionsMap(partition))
      else
        None
    }
  }

  /**
    * Returns if transaction is in map and checks it's state
 *
    * @param partition
    * @return
    */
  def getTransactionOption(partition: Int) = {
    val partOpt = getTransactionOptionNaive(partition)
    val txnOpt = {
      if (partOpt.isDefined) {
        if (partOpt.get.isClosed) {
          None
        }
        else {
          partOpt
        }
      }
      else {
        None
      }
    }
    txnOpt
  }

  /**
    * Awaits while a transaction for specified partition will be materialized
 *
    * @param partition
    * @param policy
    * @return
    */
  def awaitOpenTransactionMaterialized(partition: Int, policy: ProducerPolicy): () => Unit = {
    val partOpt = getTransactionOption(partition)
    if (partOpt.isDefined) {
      partOpt.get.awaitMaterialized()
    }
    var action: () => Unit = null
    if (partOpt.isDefined) {
      if (!partOpt.get.isClosed) {
        policy match {
          case NewTransactionProducerPolicy.CheckpointIfOpened =>
            action = () => partOpt.get.checkpoint()

          case NewTransactionProducerPolicy.CancelIfOpened =>
            action = () => partOpt.get.cancel()

          case NewTransactionProducerPolicy.CheckpointAsyncIfOpened =>
            action = () => partOpt.get.checkpoint(isSynchronous = false)

          case NewTransactionProducerPolicy.ErrorIfOpened =>
            throw new IllegalStateException(s"Previous transaction was not closed")
        }
      }
    }
    action
  }

  /**
    * Adds new transaction
 *
    * @param partition
    * @param transaction
    * @return
    */
  def put(partition: Int, transaction: Transaction[T]) = {
    PeerAgent.logger.info("put")
    this.synchronized {
      openTransactionsMap.put(partition, transaction)
    }
  }

  /**
    * Clears all transactions.
    */
  def clear() = this.synchronized {
    openTransactionsMap.clear()
  }

}
