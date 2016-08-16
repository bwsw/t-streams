package com.bwsw.tstreams.agents.producer

import com.bwsw.tstreams.converter.IConverter
import com.bwsw.tstreams.data.IStorage

/**
  * Created by ivan on 15.08.16.
  */
class TransactionData[USERTYPE](txn: Transaction[USERTYPE], ttl: Int, storage: IStorage[Array[Byte]]) {
  var items = new scala.collection.mutable.ListBuffer[Array[Byte]]()
  var lastOffset: Int = 0

  def put(elt: USERTYPE, converter: IConverter[USERTYPE, Array[Byte]]): Int = this.synchronized {
    items += converter.convert(elt)
    lastOffset += 1
    return lastOffset
  }

  def save(): () => Unit = this.synchronized {
    val job = storage.save(
      txn.getTransactionUUID(),
      txn.getTransactionOwner().stream.name,
      txn.getPartition,
      ttl,
      lastOffset,
      items)

    items = new scala.collection.mutable.ListBuffer[Array[Byte]]()
    job
  }
}
