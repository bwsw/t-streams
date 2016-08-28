package com.bwsw.tstreams.agents.producer

import java.util.concurrent.ConcurrentHashMap

import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy.ProducerPolicy

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._


/**
  * Created by Ivan Kudryavtsev on 28.08.16.
  */
class OpenTransactionsKeeper[T] {
  private val openTransactionsMap     = new ConcurrentHashMap[Int, Transaction[T]]()

  /**
    * Allows to do smth with all not closed transactions.
    *
    * @param f
    * @tparam RV
    * @return
    */
  def forallKeysDo[RV](f: (Int, Transaction[T]) => RV): Iterable[RV] = {
    val keys = openTransactionsMap.keys()
    val res = ListBuffer[RV]()
    for(k <- keys) {
      val v = openTransactionsMap.getOrDefault(k, null)
      if(v != null && !v.isClosed) {
        res.append(f(k, v))
      }
    }
    res
  }

  /**
    * Returns if transaction is in map without checking if it's not closed
    *
    * @param partition
    * @return
    */
  def getTransactionOptionNaive(partition: Int) = {
    val itm = openTransactionsMap.getOrDefault(partition, null)
    if(itm != null)
      Option(itm)
    else
      None
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
    openTransactionsMap.put(partition, transaction)
  }

  /**
    * Clears all transactions.
    */
  def clear() = {
    openTransactionsMap.clear()
  }

}
