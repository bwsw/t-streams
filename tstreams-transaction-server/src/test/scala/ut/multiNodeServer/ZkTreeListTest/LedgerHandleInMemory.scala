package ut.multiNodeServer.ZkTreeListTest

import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.LedgerHandle
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.{Record, RecordWithIndex}

class LedgerHandleInMemory(id: Long,
                           time: Long)
  extends LedgerHandle(id) {
  @volatile private var isClosed = false
  private val entryIDGen = new AtomicLong(0L)
  private val storage =
    new scala.collection.concurrent.TrieMap[Long, Record]()

  override def addRecord(data: Record): Long = {
    if (isClosed)
      throw new IllegalAccessError()

    val id = entryIDGen.getAndIncrement()
    storage.put(id, data)
    id
  }

  override def getRecord(id: Long): Record =
    storage.get(id).orNull

  override def lastRecordID(): Long =
    entryIDGen.get() - 1L

  override def lastRecord(): Option[Record] = {
    storage.get(lastRecordID())
  }

  override def readRecords(from: Long, to: Long): Array[Record] = {
    val fromCorrected =
      if (from < 0L)
        0L
      else
        from

    val dataNumber = scala.math.abs(to - fromCorrected + 1)
    val data = new Array[Record](dataNumber.toInt)

    var index = 0
    var toReadIndex = fromCorrected
    while (index < dataNumber) {
      data(index) = storage(toReadIndex)
      index = index + 1
      toReadIndex = toReadIndex + 1
    }
    data
  }

  override def getOrderedRecords(from: Long): Array[RecordWithIndex] = {
    val fromCorrected =
      if (from < 0L)
        0L
      else
        from

    val lastRecord = lastRecordID()
    val indexes = fromCorrected to lastRecord

    readRecords(fromCorrected, lastRecord)
      .zip(indexes).sortBy(_._1.timestamp)
      .map {case (record, index) =>
        RecordWithIndex(index, record)
      }
  }

  override def close(): Unit = {
    isClosed = true
  }

  override val getCreationTime: Long = {
    time
  }
}
