package com.bwsw.tstreams.common

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

import net.openhft.chronicle.queue.{ExcerptTailer, ExcerptAppender, ChronicleQueueBuilder}
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue

/**
  * Created by ivan on 19.08.16.
  */
abstract class AbstractPersistentQueue[T](basePath: String) extends AbstractQueue[T] {
  val q: SingleChronicleQueue = ChronicleQueueBuilder
    .single(java.nio.file.Paths.get(basePath, "q1").toString)
    .build()

  private val appender: ExcerptAppender = q.createAppender()
  private val getter: ExcerptTailer     = q.createTailer()



  /**
    * Queue blocking stuff
    */
  private val mutex = new ReentrantLock(true)
  private val cond = mutex.newCondition()

  override def put(elt: T): Unit = {
    LockUtil.withLockOrDieDo[Unit](mutex, (100, TimeUnit.SECONDS), None, () => {
      appender.writeText(serialize(elt))
      cond.signal()
    })
  }

  override def get(): T = {
    LockUtil.withLockOrDieDo[T](mutex, (100, TimeUnit.SECONDS), None, () => {
      val data: T = deserialize(getter.readText())
      if(data == null) {
        cond.await()
        deserialize(getter.readText())
      } else
        data
    })
  }

  private def deserialize(s: String): T = ???
  private def serialize(elt: T): String = ???
}
