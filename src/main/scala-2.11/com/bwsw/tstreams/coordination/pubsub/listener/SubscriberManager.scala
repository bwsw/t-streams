package com.bwsw.tstreams.coordination.pubsub.listener

import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreams.coordination.pubsub.messages.ProducerTopicMessage

import scala.collection.mutable.ListBuffer

class SubscriberManager {
  private val callbacks = new ListBuffer[(ProducerTopicMessage)=>Unit]()
  private var count = 0
  private val lockCount = new ReentrantLock(true)
  private val lockCallbacks = new ReentrantLock(true)

  def addCallback(callback : (ProducerTopicMessage) => Unit) = {
    lockCallbacks.lock()
    callbacks += callback
    lockCallbacks.unlock()
  }

  def getCount() : Int = {
    var res = -1
    lockCount.lock()
    res = count
    lockCount.unlock()
    assert(res != -1)
    res
  }

  def resetCount() : Unit = {
    lockCount.lock()
    count = 0
    lockCount.unlock()
  }

  def invokeCallbacks(msg : ProducerTopicMessage) : Unit = {
    lockCallbacks.lock()
    callbacks.foreach(x=>x(msg))
    lockCallbacks.unlock()
  }

  def incrementCount() : Unit = {
    lockCount.lock()
    count += 1
    lockCount.unlock()
  }
}
