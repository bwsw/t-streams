package com.bwsw.tstreams.agents.producer

import java.util.concurrent.ConcurrentHashMap

import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy.ProducerPolicy

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer


/**
  * Created by Ivan Kudryavtsev on 28.08.16.
  */
class OpenTransactionsKeeper {
  private val openTransactionsMap = new ConcurrentHashMap[Int, IProducerTransaction]()

  /**
    * Allows to do something with all not closed transactions.
    *
    * @param f
    * @tparam RV
    * @return
    */
  def forallKeysDo[RV](f: (Int, IProducerTransaction) => RV): Iterable[RV] = {
    val keys = openTransactionsMap.keys().asScala
    val res = ListBuffer[RV]()
    for (k <- keys) {
      val v = openTransactionsMap.getOrDefault(k, null)
      if (v != null && !v.isClosed) {
        try {
          res.append(f(k, v))
        } catch {
          case e: IllegalStateException =>
          // since forall is not atomic specific transactions can be switched to another state.
        }
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
  private def getTransactionOptionNaive(partition: Int) =
    Option(openTransactionsMap.getOrDefault(partition, null))

  /**
    * Returns if transaction is in map and checks it's state
    *
    * @param partition
    * @return
    */
  def getTransactionOption(partition: Int) = {
    val transactionOpt = getTransactionOptionNaive(partition)
    transactionOpt.flatMap(transaction => if (!transaction.isClosed()) Some(transaction) else None)
  }

  /**
    * Awaits while a transaction for specified partition will be materialized
    *
    * @param partition
    * @param policy
    * @return
    */
  def handlePreviousOpenTransaction(partition: Int, policy: ProducerPolicy): () => Unit = {
    val partOpt = getTransactionOption(partition)

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
  def put(partition: Int, transaction: IProducerTransaction) = {
    openTransactionsMap.put(partition, transaction)
  }

  /**
    * Clears all transactions.
    */
  def clear() = {
    openTransactionsMap.clear()
  }

}
