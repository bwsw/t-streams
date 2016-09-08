package com.bwsw.tstreams.common

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
  * Created by Ivan Kudryavtsev on 19.08.16.
  */
trait AbstractQueue[T] {

  protected val inFlight = new AtomicInteger(0)

  def put(elt: T): Unit = ???
  def get(delay: Long, units: TimeUnit): T = ???
  def getInFlight(): Int = inFlight.get()
}
