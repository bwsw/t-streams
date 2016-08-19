package com.bwsw.tstreams.common

/**
  * Created by ivan on 19.08.16.
  */
trait AbstractQueue[T] {
  def put(elt: T): Unit = ???
  def get(): T = ???
}
