package com.bwsw.tstreams.generator

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

/**
  * Entity for generating new transaction time
  */
class LocalTransactionGenerator(val scale: Int = 10000) extends ITransactionGenerator {

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
    now * scale + counter.getAndIncrement()
  }

  /**
    * @return time based on transaction
    */
  override def getTransaction(timestamp: Long) = timestamp * scale
}