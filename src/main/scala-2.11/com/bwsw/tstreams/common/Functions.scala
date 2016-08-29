package com.bwsw.tstreams.common

import org.slf4j.LoggerFactory

/**
  * Created by ivan on 29.08.16.
  */
object Functions {
  val logger = LoggerFactory.getLogger(this.getClass)

  def calculateThreadAmount(minThreads: Int, maxThreads: Int): Int = {
    if (minThreads >= maxThreads) {
      logger.warn(s"User requested ${minThreads} worker threads, but total partitions amount is ${maxThreads}. Will use ${maxThreads}")
      return maxThreads
    }

    if(minThreads <= 0) {
      logger.warn(s"User requested ${minThreads} worker threads, but minimal amount is 1. Will use 1 worker thread.")
      return 1
    }

    if(maxThreads % minThreads == 0) {
      return minThreads
    }

    for(i <- minThreads to maxThreads) {
      if (maxThreads % i == 0)
        return i
    }
    return maxThreads
  }
}
