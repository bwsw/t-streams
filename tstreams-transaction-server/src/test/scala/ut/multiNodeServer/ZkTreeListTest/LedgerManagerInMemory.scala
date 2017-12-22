package ut.multiNodeServer.ZkTreeListTest

import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.{LedgerHandle, LedgerManager}

class LedgerManagerInMemory
  extends LedgerManager{

  private val ledgerIDGen = new AtomicLong(0L)

  private val storage =
    new java.util.concurrent.ConcurrentHashMap[Long, LedgerHandle]()

  override def createLedger(timestamp: Long): LedgerHandle = {
    val id = ledgerIDGen.getAndIncrement()

    val previousLedger = storage.computeIfAbsent(id, {id =>
      new LedgerHandleInMemory(id, timestamp)
    })

    if (previousLedger != null)
      previousLedger
    else
      throw new IllegalArgumentException()
  }

  override def openLedger(id: Long): Option[LedgerHandle] = {
    Option(storage.get(id))
  }

  override def deleteLedger(id: Long): Boolean = {
    Option(storage.remove(id)).isDefined
  }

  override def isClosed(id: Long): Boolean = true
}
