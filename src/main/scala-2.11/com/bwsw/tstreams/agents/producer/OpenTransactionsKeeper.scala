package com.bwsw.tstreams.agents.producer

import com.bwsw.tstreams.agents.producer.NewTransactionProducerPolicy.ProducerPolicy

import scala.collection.mutable

/**
  * Created by Ivan Kudryavtsev on 28.08.16.
  */
class OpenTransactionsKeeper[T] {
  private val openTransactionsMap     = mutable.Map[Int, Transaction[T]]()

  def forallKeysDo[RV](f: (Int, Transaction[T]) => RV): Iterable[RV] = this.synchronized {
    openTransactionsMap.map(kv => f(kv._1, kv._2))
  }

  def getTransactionOptionNaive(partition: Int) = this.synchronized {
    if(openTransactionsMap.contains(partition))
      Option(openTransactionsMap(partition))
    else
      None
  }


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

  def put(partition: Int, transaction: Transaction[T]) = this.synchronized {
    openTransactionsMap.put(partition, transaction)
  }

  def clear() = this.synchronized {
    openTransactionsMap.clear()
  }

}
