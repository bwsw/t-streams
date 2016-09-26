package com.bwsw.tstreams.generator

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import com.bwsw.tstreams.metadata.TransactionDatabase

/**
  * Entity for generating new transaction time
  */
class LocalTransactionGenerator() extends ITransactionGenerator {

  var counter       = new AtomicInteger(0)
  var currentMillis = new AtomicLong(0)

  /**
    * @return Transaction ID
    */
  override def getTransaction(): Long = this.synchronized {
    val now = System.currentTimeMillis()
    if(now - currentMillis.get > 0) {
      currentMillis.set(now)
      counter.set(0)
    }
    now * TransactionDatabase.SCALE + counter.getAndIncrement()
  }

  /**
    * @return time based on transaction
    */
  override def getTransaction(timestamp: Long) = timestamp * TransactionDatabase.SCALE

}