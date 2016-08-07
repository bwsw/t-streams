package com.bwsw.tstreams.common

import java.util.concurrent.locks.AbstractQueuedSynchronizer
import java.util.concurrent.TimeUnit

/**
  * Created by ivan on 07.08.16.
  */
class ResettableCountDownLatch(val count: Int) {

  if (count < 0)
    throw new IllegalArgumentException("count < 0")

  private final class Sync(val count: Int) extends AbstractQueuedSynchronizer {
    var startCount = count
    setState(startCount)

    def getCount = getState
    override def tryAcquireShared(acquires: Int): Int = if (getState == 0) 1 else -1

    override def tryReleaseShared(releases: Int): Boolean = {
      while (true) {
        val c: Int = getState
        if (c == 0)
          return false
        val nextc: Int = c - 1
        if (compareAndSetState(c, nextc))
          return nextc == 0
      }
      false
    }

    def setValue(value: Int) = setState(value)
    def reset() = setState(startCount)
  }

  private val sync: Sync = new Sync(count)

  override def toString = super.toString + "[Count = " + sync.getCount + "]"
  def reset = sync.reset()
  def setValue(value: Int) = sync.setValue(value)
  def countDown: Unit = sync.releaseShared(1)
  def getCount = sync.getCount

  @throws[InterruptedException]
  def await(timeout: Long, unit: TimeUnit): Boolean =
    sync.tryAcquireSharedNanos(1, unit.toNanos(timeout))

  @throws[InterruptedException]
  def await() =
    sync.acquireSharedInterruptibly(1)

}
