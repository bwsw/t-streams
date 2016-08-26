package com.bwsw.tstreams.common

import java.util.concurrent.TimeUnit

/**
  * Created by ivan on 19.08.16.
  */
trait AbstractQueue[T] {
  def put(elt: T): Unit = ???
  def get(delay: Long, units: TimeUnit): T = ???
}
