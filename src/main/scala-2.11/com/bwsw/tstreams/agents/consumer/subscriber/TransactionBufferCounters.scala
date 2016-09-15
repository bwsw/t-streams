package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.concurrent.atomic.AtomicLong

/**
  * Counts events which are delivered to transaction buffer v2
  *
  * @param openEvents
  * @param cancelEvents
  * @param updateEvents
  * @param preCheckpointEvents
  * @param postCheckpointEvents
  */
case class TransactionBufferCounters(openEvents: AtomicLong          = new AtomicLong(0),
                                     cancelEvents: AtomicLong         = new AtomicLong(0),
                                     updateEvents: AtomicLong         = new AtomicLong(0),
                                     preCheckpointEvents: AtomicLong  = new AtomicLong(0),
                                     postCheckpointEvents: AtomicLong = new AtomicLong(0)) {
  def dump(partition: Int): Unit = {
    Subscriber.logger.info(s"Partitions $partition - Open Events received: ${openEvents.get()}")
    Subscriber.logger.info(s"Partitions $partition - Cancel Events received: ${cancelEvents.get()}")
    Subscriber.logger.info(s"Partitions $partition - Update Events received: ${updateEvents.get()}")
    Subscriber.logger.info(s"Partitions $partition - PreCheckpoint Events received: ${preCheckpointEvents.get()}")
    Subscriber.logger.info(s"Partitions $partition - PostCheckpoint Events received: ${postCheckpointEvents.get()}")
  }
}