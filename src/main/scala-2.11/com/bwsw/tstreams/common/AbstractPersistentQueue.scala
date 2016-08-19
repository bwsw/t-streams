package com.bwsw.tstreams.common

import java.nio.file.Files
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

import net.openhft.chronicle.queue.{ExcerptTailer, ExcerptAppender, ChronicleQueueBuilder}
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueue

/**
  * Created by ivan on 19.08.16.
  */
abstract class AbstractPersistentQueue[T](basePath: String) extends AbstractQueue[T] {
  Files.createDirectories(java.nio.file.Paths.get(basePath))
  val q: SingleChronicleQueue = ChronicleQueueBuilder
    .single(java.nio.file.Paths.get(basePath, "queue").toString)
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
      appender.writeText(serialize(elt.asInstanceOf[Object]))
      cond.signal()
    })
  }

  override def get(delay: Long, units: TimeUnit): T = {
    LockUtil.withLockOrDieDo[T](mutex, (100, TimeUnit.SECONDS), None, () => {
      val raw = getter.readText()
      if(raw == null) {
        if(cond.await(delay, units))
          deserialize(getter.readText()).asInstanceOf[T]
        else
          null.asInstanceOf[T]
      } else
        deserialize(raw).asInstanceOf[T]
    })
  }

  protected def deserialize(s: String): Object = ???
  protected def serialize(elt: Object): String = ???
}
