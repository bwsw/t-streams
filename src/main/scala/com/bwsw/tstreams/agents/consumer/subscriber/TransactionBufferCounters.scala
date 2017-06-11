package com.bwsw.tstreams.agents.consumer.subscriber

import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreamstransactionserver.protocol.TransactionState

/**
  * Counts events which are delivered to transaction buffer v2
  *
  * @param openEvents
  * @param cancelEvents
  * @param updateEvents
  * @param checkpointEvents
  */
private[tstreams] case class TransactionBufferCounters(openEvents: AtomicLong = new AtomicLong(0),
                                     cancelEvents: AtomicLong = new AtomicLong(0),
                                     updateEvents: AtomicLong = new AtomicLong(0),
                                     checkpointEvents: AtomicLong = new AtomicLong(0),
                                     instantEvents: AtomicLong = new AtomicLong(0)) {
  def dump(partition: Int): Unit = {
    Subscriber.logger.info(s"Partitions $partition - Open Events received: ${openEvents.get()}")
    Subscriber.logger.info(s"Partitions $partition - Cancel Events received: ${cancelEvents.get()}")
    Subscriber.logger.info(s"Partitions $partition - Update Events received: ${updateEvents.get()}")
    Subscriber.logger.info(s"Partitions $partition - Checkpoint Events received: ${checkpointEvents.get()}")
    Subscriber.logger.info(s"Partitions $partition - Instant Events received: ${instantEvents.get()}")
  }


  private[subscriber] def updateStateCounters(state: TransactionState) =
    state.status match {
      case TransactionState.Status.Opened => openEvents.incrementAndGet()
      case TransactionState.Status.Cancelled => cancelEvents.incrementAndGet()
      case TransactionState.Status.Updated => updateEvents.incrementAndGet()
      case TransactionState.Status.Checkpointed => checkpointEvents.incrementAndGet()
      case TransactionState.Status.Instant => instantEvents.incrementAndGet()
      case _ =>
    }
}