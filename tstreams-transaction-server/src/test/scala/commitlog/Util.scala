package commitlog

import java.util.concurrent.atomic.AtomicLong

import com.bwsw.commitlog.IDGenerator

private[commitlog] object Util {

  final class IDGeneratorInMemory
    extends IDGenerator[Long] {

    private val iDGenerator =
      new AtomicLong(0L)

    override def nextID: Long =
      iDGenerator.getAndIncrement()

    override def currentID: Long =
      iDGenerator.get()
  }

  def createIDGenerator =
    new IDGeneratorInMemory
}
