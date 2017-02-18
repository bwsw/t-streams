package com.bwsw.tstreams.common

case class IntMinMaxDefault(min: Int = 0, max: Int = 0, default: Int = 0) {
  def check(v: Int) = {
    assert(v >= min && v <= max)
    v
  }
}