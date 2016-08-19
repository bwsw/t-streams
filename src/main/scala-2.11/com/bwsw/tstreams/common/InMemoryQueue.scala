package com.bwsw.tstreams.common

import java.util.concurrent.LinkedBlockingQueue

/**
  * Created by ivan on 19.08.16.
  */
class InMemoryQueue[T] extends AbstractQueue[T] {
  val q = new LinkedBlockingQueue[T]()
  override def put(elt: T) = q.put(elt)
  override def get(): T = q.take()
}
