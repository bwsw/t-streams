package com.bwsw.tstreams.common

import java.util.concurrent.TimeUnit
import java.util.concurrent.LinkedBlockingQueue

/**
  * Created by Ivan Kudryavtsev on 19.08.16.
  */
class InMemoryQueue[T] extends AbstractQueue[T] {
  /**
    * Queue blocking stuff
    */
  val q = new LinkedBlockingQueue[T]()

  override def put(elt: T) = {
    q.put(elt)
    inFlight.incrementAndGet()
  }

  override def get(delay: Long, units: TimeUnit): T = {
    val r = q.poll(delay, units)
    if(r != null) inFlight.decrementAndGet()
    r
}
}
