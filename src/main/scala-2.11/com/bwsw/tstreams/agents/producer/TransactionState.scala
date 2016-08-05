package com.bwsw.tstreams.agents.producer

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.ResettableCountDownLatch

/**
  * Stores state of transaction
  */
class TransactionState {
  /**
    * This latch is used to await when master will materialize the Transaction.
    * Before materialization complete checkpoints, updates, cancels are not permitted.
    */
  val materialize = new CountDownLatch(1)

  /**
    * This atomic used to make update exit if Transaction is not materialized
    */
  val isMaterialized = new AtomicBoolean(false)

  /**
    * Variable for indicating transaction state
    */
  private val closed = new AtomicBoolean(false)

  /**
    * This special trigger is used to avoid a very specific race, which could happen if
    * checkpoint/cancel will be called same time when update will do update. So, we actully
    * must protect from this situation, that's why during update, latch must be set to make
    * cancel/checkpoint wait until update will complete
    * We also need special test for it.
    */
  private val updateSignalVar = new ResettableCountDownLatch(0)

  /**
    * changes transaction state to updating
    */
  def setUpdateInProgress = updateSignalVar.setValue(1)

  /**
    * changed transaction state to finished updating
    */
  def setUpdateFinished = updateSignalVar.countDown()

  /**
    * await while transaction updates
    * @return
    */
  def awaitUpdateComplete: Boolean = updateSignalVar.await(10, TimeUnit.SECONDS)

  /**
    * make transaction closed
    */
  def close = closed.getAndSet(true)

  /**
    * get transaction closed state
    */
  def isClosed = closed.get()
}
