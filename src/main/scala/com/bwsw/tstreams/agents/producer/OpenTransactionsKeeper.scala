package com.bwsw.tstreams.agents.producer

import com.bwsw.tstreams.agents.producer.NewProducerTransactionPolicy.ProducerPolicy

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


/**
  * Created by Ivan Kudryavtsev on 28.08.16.
  */
class OpenTransactionsKeeper {
  private val openTransactionsMap = mutable.Map[Int, (Long, mutable.Set[IProducerTransaction])]()

  /**
    * Allows to do something with all not closed transactions.
    *
    * @param f
    * @tparam RV
    * @return
    */
  def forallKeysDo[RV](f: (Int, IProducerTransaction) => RV): Iterable[RV] = openTransactionsMap.synchronized {
    val res = ListBuffer[RV]()
    openTransactionsMap.keys.foreach(partition =>
      openTransactionsMap.get(partition)
        .foreach(txnSetValue => {
          res ++= txnSetValue._2.map(txn => f(partition, txn))
        }))
    res
  }

  /**
    * Returns if transaction is in map without checking if it's not closed
    *
    * @param partition
    * @return
    */
  private[tstreams] def getTransactionSetOption(partition: Int) = openTransactionsMap.synchronized {
    openTransactionsMap.get(partition)
  }

  /**
    * Awaits while a transaction for specified partition will be materialized
    *
    * @param partition
    * @param policy
    * @return
    */
  def handlePreviousOpenTransaction(partition: Int, policy: ProducerPolicy): () => Unit = openTransactionsMap.synchronized {
    val transactionSetOption = getTransactionSetOption(partition)

    val allClosed = transactionSetOption.forall(transactionSet => transactionSet._2.forall(_.isClosed))
    if (!allClosed) {
      policy match {
        case NewProducerTransactionPolicy.CheckpointIfOpened =>
          () => transactionSetOption.get._2.foreach(txn => if (!txn.isClosed) txn.checkpoint())

        case NewProducerTransactionPolicy.CancelIfOpened =>
          () => transactionSetOption.get._2.foreach(txn => if (!txn.isClosed) txn.cancel())

        case NewProducerTransactionPolicy.EnqueueIfOpened => () => Unit

        case NewProducerTransactionPolicy.ErrorIfOpened =>
          throw new IllegalStateException(s"Previous transaction was not closed")
      }
    } else {
      () => Unit
    }
  }

  /**
    * Adds new transaction
    *
    * @param partition
    * @param transaction
    * @return
    */
  def put(partition: Int, transaction: IProducerTransaction) = openTransactionsMap.synchronized {
    val transactionSetValueOpt = openTransactionsMap.get(partition)
    if (transactionSetValueOpt.isEmpty) {
      openTransactionsMap.put(partition, (0, mutable.Set[IProducerTransaction](transaction)))
    } else {
      val nextTransactionID = transaction.getTransactionID
      openTransactionsMap
        .put(partition, (nextTransactionID,
          openTransactionsMap.get(partition).get._2 + transaction))
    }
  }

  def remove(partition: Int, transaction: IProducerTransaction) = openTransactionsMap.synchronized {
    openTransactionsMap.get(partition).map(transactionSetValue => transactionSetValue._2.remove(transaction))
  }

  /**
    * Clears all transactions.
    */
  def clear() = openTransactionsMap.synchronized {
    openTransactionsMap.clear()
  }

}
