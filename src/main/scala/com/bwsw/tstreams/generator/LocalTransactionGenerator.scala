package com.bwsw.tstreams.generator

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

object LocalTransactionGenerator {
  val SCALE = 100000
}

/**
  * Entity for generating new transaction time
  */
class LocalTransactionGenerator() extends ITransactionGenerator {

  var counter = new AtomicInteger(1)
  var currentMillis = new AtomicLong(0)

  /**
    * @return Transaction ID
    */
  override def getTransaction(): Long = this.synchronized {
    val now = System.currentTimeMillis()
    if (now - currentMillis.get > 0) {
      currentMillis.set(now)
      counter.set(1)
    }
    now * LocalTransactionGenerator.SCALE + counter.getAndIncrement()
  }

  /**
    * @return time based on transaction
    */
  override def getTransaction(timestamp: Long) = timestamp * LocalTransactionGenerator.SCALE

}