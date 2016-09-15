package com.bwsw.tstreams.coordination.subscriber

import java.util.concurrent.atomic.AtomicInteger

import com.bwsw.tstreams.coordination.messages.state.TransactionStateMessage

import scala.collection.mutable.ListBuffer

/**
  *
  */
class CallbackManager {
  private val callbacks = new ListBuffer[(TransactionStateMessage) => Unit]()
  private val count = new AtomicInteger(0)

  def addCallback(callback: (TransactionStateMessage) => Unit) = this.synchronized {
    callbacks += callback
  }

  def invokeCallbacks(msg: TransactionStateMessage): Unit = this.synchronized {
    callbacks.foreach(x => x(msg))
  }

  def getCount(): Int = count.get()

  def resetCount(): Unit = count.set(0)

  def incrementCount(): Unit = count.incrementAndGet()

}
