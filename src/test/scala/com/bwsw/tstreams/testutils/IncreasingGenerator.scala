package com.bwsw.tstreams.testutils

import java.util.concurrent.atomic.AtomicLong

/**
  * Created by ivan on 09.06.17.
  */
object IncreasingGenerator {
  val id = new AtomicLong(0)
  def get = id.incrementAndGet()
}
