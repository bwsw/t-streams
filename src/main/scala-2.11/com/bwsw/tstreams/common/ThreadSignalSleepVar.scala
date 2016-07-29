package com.bwsw.tstreams.common

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

/**
  * Created by ivan on 25.07.16.
  */
class ThreadSignalSleepVar[T](size: Int = 1) {
  val signalQ = new LinkedBlockingQueue[T](size)

  def wait(timeout: Int, unit: TimeUnit = TimeUnit.MILLISECONDS): T = signalQ.poll(timeout, unit)

  def signal(value: T) = signalQ.put(value)
}
