package com.bwsw.commitlog

trait IDGenerator[T] {
  def nextID: T

  def currentID: T
}
