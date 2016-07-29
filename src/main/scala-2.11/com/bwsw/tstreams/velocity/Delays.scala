package com.bwsw.tstreams.velocity

object Delays {
  private val arr = new Array[Long](5)

  def addDelay(d1: Long, d2: Long, d3: Long, d4: Long, d5: Long) = {
    arr(0) += d1
    arr(1) += d2
    arr(2) += d3
    arr(3) += d4
    arr(4) += d5
  }

  private def clear() = {
    arr(0) = 0
    arr(1) = 0
    arr(2) = 0
    arr(3) = 0
    arr(4) = 0
  }

  def printAndClear() = {
    println(s"delays={${arr(0)},${arr(1)},${arr(2)},${arr(3)},${arr(4)}}")
    this.clear()
  }
}
