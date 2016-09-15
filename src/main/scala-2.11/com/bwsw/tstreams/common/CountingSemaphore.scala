package com.bwsw.tstreams.common

/**
  * Created by Ivan Kudryavtsev on 15.08.16.
  */
class CountingSemaphore(initial: Int = 0) {
  private var signals: Int = initial

  def take(): Int = this.synchronized {
    while (this.signals == 0) wait()
    signals -= 1
    signals
  }

  def release(): Int = this.synchronized {
    signals += 1
    notify()
    signals
  }

  def waitZero() = this.synchronized {
    while (this.signals != 0) wait()
  }
}

