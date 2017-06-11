package com.bwsw.tstreams.common

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

/**
  * Created by Ivan Kudryavtsev on 19.08.16.
  */
class MemoryQueue[T] extends Queue[T] {
  val queue = new LinkedBlockingQueue[T]()

  override def put(elt: T) = {
    queue.put(elt)
    inFlight.incrementAndGet()
  }

  override def get(delay: Long, units: TimeUnit): T = {
    val r = queue.poll(delay, units)
    if (r != null) inFlight.decrementAndGet()
    r
  }
}
