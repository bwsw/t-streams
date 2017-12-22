package com.bwsw.tstreamstransactionserver.netty.server.db.rocks

import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDbIterator
import org.rocksdb.RocksIterator

private[rocks] final class RocksDbIteratorWrapper(iterator: RocksIterator)
  extends KeyValueDbIterator {
  override def key(): Array[Byte] = iterator.key()

  override def value(): Array[Byte] = iterator.value()

  override def isValid: Boolean = iterator.isValid

  override def seekToFirst(): Unit = iterator.seekToFirst()

  override def seekToLast(): Unit = iterator.seekToLast()

  override def seek(target: Array[Byte]): Unit = iterator.seek(target)

  override def next(): Unit = iterator.next()

  override def prev(): Unit = iterator.prev()

  override def close(): Unit = iterator.close()
}
