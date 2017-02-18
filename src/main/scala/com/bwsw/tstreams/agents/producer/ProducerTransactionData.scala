package com.bwsw.tstreams.agents.producer

/**
  * Created by Ivan Kudryavtsev on 15.08.16.
  */
class ProducerTransactionData(transaction: ProducerTransaction, ttl: Int, storage: IStorage[Array[Byte]]) {
  var items = new scala.collection.mutable.ListBuffer[Array[Byte]]()
  var lastOffset: Int = 0

  def put(elt: Array[Byte]): Int = this.synchronized {
    items += elt
    lastOffset += 1
    return lastOffset
  }

  def save(): () => Unit = this.synchronized {
    val job = storage.save(
      transaction.getTransactionID(),
      transaction.getTransactionOwner().stream.name,
      transaction.getPartition,
      ttl,
      lastOffset,
      items)

    items = new scala.collection.mutable.ListBuffer[Array[Byte]]()
    job
  }
}
