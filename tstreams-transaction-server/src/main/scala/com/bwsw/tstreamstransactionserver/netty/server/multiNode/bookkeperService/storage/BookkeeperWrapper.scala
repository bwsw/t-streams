package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.storage

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.{LedgerHandle, LedgerManager}
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.BookkeeperOptions
import org.apache.bookkeeper.client.BookKeeper

import scala.util.Try

class BookkeeperWrapper(bookKeeper: BookKeeper,
                        bookkeeperOptions: BookkeeperOptions)
  extends LedgerManager {

  override def createLedger(timestamp: Long): LedgerHandle = {

    val metadata =
      new java.util.HashMap[String, Array[Byte]]

    val size =
      java.lang.Long.BYTES
    val buffer =
      java.nio.ByteBuffer
        .allocate(size)
        .putLong(timestamp)
    buffer.flip()

    val bytes = if (buffer.hasArray)
      buffer.array()
    else {
      val bytes = new Array[Byte](size)
      buffer.get(bytes)
      bytes
    }

    metadata.put(LedgerHandle.KeyTime, bytes)

    val ledgerHandle = bookKeeper.createLedger(
      bookkeeperOptions.ensembleNumber,
      bookkeeperOptions.writeQuorumNumber,
      bookkeeperOptions.ackQuorumNumber,
      BookKeeper.DigestType.MAC,
      bookkeeperOptions.password,
      metadata
    )
    new BookKeeperLedgerHandleWrapper(ledgerHandle)
  }

  override def openLedger(id: Long): Option[LedgerHandle] = {
    val ledgerHandleTry = Try(bookKeeper.openLedgerNoRecovery(
      id,
      BookKeeper.DigestType.MAC,
      bookkeeperOptions.password
    ))
    ledgerHandleTry.map(ledgerHandle =>
      new BookKeeperLedgerHandleWrapper(ledgerHandle)
    ).toOption
  }

  override def deleteLedger(id: Long): Boolean = {
    Try(bookKeeper.deleteLedger(id)).isSuccess
  }

  override def isClosed(id: Long): Boolean = {
    Try(bookKeeper.isClosed(id)).getOrElse(false)
  }
}
