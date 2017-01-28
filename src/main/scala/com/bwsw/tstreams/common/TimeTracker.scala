package com.bwsw.tstreams.common

import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Created by Ivan Kudryavtsev on 11.08.16.
  */
object TimeTracker {
  val isEnabled = true
  val trackMap = mutable.Map[String, (Long /* acc */ , Long /* first */ , Long, Long)]()
  val logger = LoggerFactory.getLogger(this.getClass)


  def update_start(key: String): Unit = if (isEnabled)
    this.synchronized {
      val opt = trackMap.get(key)
      val time = System.currentTimeMillis()
      if (opt.isDefined)
        trackMap(key) = (opt.get._1, time, opt.get._3, opt.get._4)
      else
        trackMap(key) = (0, time, 0, 0)
    }

  def update_end(key: String): Unit = if (isEnabled) {
    val opt = trackMap.get(key)
    val time = System.currentTimeMillis()
    if (opt.isEmpty)
      throw new IllegalStateException(s"Wrong usage - end before start: $key")
    val delta = if (time - opt.get._2 > 1000) 0 else time - opt.get._2
    trackMap(key) = (
      opt.get._1 + delta,
      0,
      opt.get._3 + 1,
      if (delta > opt.get._4) delta else opt.get._4)
    //dump()
  }

  def dump() = if (isEnabled)
    this.synchronized {
      trackMap.foreach(kv => logger.info(s"Metric '${kv._1}' -> Total time '${kv._2._1}', Max '${kv._2._4}', Count '${kv._2._3}'"))
    }
}
