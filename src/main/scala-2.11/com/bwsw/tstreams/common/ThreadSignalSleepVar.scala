package com.bwsw.tstreams.common

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

/**
  * Created by Ivan Kudryavtsev on 25.07.16.
  * Allows wait for external event in thread for specified amount of time (combines sleep and flag)
  */
class ThreadSignalSleepVar[T](size: Int = 1) {
  val signalQ = new LinkedBlockingQueue[T](size)

  /**
    * waits for event for specified amount of time
    * @param timeout timeout to wait
    * @param unit TimeUnit information
    * @return
    */
  def wait(timeout: Int, unit: TimeUnit = TimeUnit.MILLISECONDS): T = signalQ.poll(timeout, unit)

  /**
    * signals about new event
    * @param value
    */
  def signal(value: T) = signalQ.put(value)
}
