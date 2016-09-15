package com.bwsw.tstreams.agents.producer

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreams.common.ResettableCountDownLatch

class MaterializationException(s: String = "")
  extends IllegalStateException(s: String)

/**
  * Stores state of transaction
  */
class ProducerTransactionState {
  /**
    * Start time is when State object (Transaction object) was created.
    * This value is used to do delayed transaction materialization.
    */
  private val startTime = System.currentTimeMillis()
  private var materializationTime: Long = 0

  /**
    * This latch is used to await when master will materialize the Transaction.
    * Before materialization complete checkpoints, updates, cancels are not permitted.
    */
  val materialize = new CountDownLatch(1)

  /**
    * This atomic used to make update exit if Transaction is not materialized
    */
  val materialized = new AtomicBoolean(false)

  /**
    * Makes transaction materialized (which means that the transaction now can be updated, cancelled, checkpointed)
    */
  def makeMaterialized() = {
    if (materialized.getAndSet(true))
      throw new IllegalStateException("State is materialized already. Wrong operation")
    materialize.countDown()
    this.synchronized {
      materializationTime = System.currentTimeMillis()
    }
  }

  /**
    * waits until the transaction will be materialized (blocker for checkpoint, cancel)
    * or
    */
  def awaitMaterialization(masterTimeout: Int) = {

    def throwExc = throw new MaterializationException(s"Master didn't materialized the transaction during $masterTimeout.")

    val mtMs = masterTimeout * 1000
    val mt = this.synchronized {
      if (0 == materializationTime)
        System.currentTimeMillis()
      else
        materializationTime
    }

    val materializationSpent = mt - startTime

    if (mtMs <= materializationSpent)
      throwExc

    if (!materialize.await(mtMs - materializationSpent, TimeUnit.MILLISECONDS))
      throwExc
  }

  /**
    * check if the transaction is materialized
    */
  def isMaterialized(): Boolean = materialized.get

  /**
    * Variable for indicating transaction state
    */
  private val closed = new AtomicBoolean(false)

  /**
    * This special trigger is used to avoid a very specific race, which could happen if
    * checkpoint/cancel will be called same time when update will do update. So, we actually
    * must protect from this situation, that's why during update, latch must be set to make
    * cancel/checkpoint wait until update will complete
    * We also need special test for it.
    */
  private val updateSignalVar = new ResettableCountDownLatch(0)

  /**
    * changes transaction state to updating
    */
  def setUpdateInProgress() = updateSignalVar.setValue(1)

  /**
    * changes transaction state to finished updating
    */
  def setUpdateFinished() = updateSignalVar.countDown

  /**
    * awaits while transaction updates
    *
    * @return
    */
  def awaitUpdateComplete(): Unit = {
    if (!updateSignalVar.await(10, TimeUnit.SECONDS))
      throw new IllegalStateException("Update takes too long (> 10 seconds). Probably failure.")
  }

  /**
    * Makes transaction closed
    */
  def closeOrDie(): Unit = {
    if (closed.getAndSet(true))
      throw new IllegalStateException("Transaction state is already closed. Wrong operation. Double close.")
  }

  /**
    * Ensures that transaction is still open
    */
  def isOpenedOrDie(): Unit = {
    if (closed.get)
      throw new IllegalStateException("Transaction state is already closed. Wrong operation.")
  }

  /**
    * get transaction closed state
    */
  def isClosed() = closed.get()
}
