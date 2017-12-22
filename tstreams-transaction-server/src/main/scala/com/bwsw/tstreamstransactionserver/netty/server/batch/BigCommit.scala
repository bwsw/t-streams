package com.bwsw.tstreamstransactionserver.netty.server.batch

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreamstransactionserver.netty.server.RocksWriter
import com.bwsw.tstreamstransactionserver.netty.server.consumerService.ConsumerTransactionRecord
import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.ProducerTransactionRecord
import org.slf4j.{Logger, LoggerFactory}

object BigCommit {
  val bookkeeperKey: Array[Byte] = "key".getBytes()
  val commitLogKey: Array[Byte] = "file".getBytes()
}

class BigCommit(rocksWriter: RocksWriter,
                databaseIndex: Int,
                key: Array[Byte],
                value: Array[Byte]) {

  private val isFixed =
    new AtomicBoolean(false)

  private val logger: Logger =
    LoggerFactory.getLogger(this.getClass)

  private val batch = {
    rocksWriter.getNewBatch
  }

  def putProducerTransactions(producerTransactions: Seq[ProducerTransactionRecord]): Unit = {
    val isFixedNow =
      isFixed.get()

    if (!isFixedNow) {
      if (logger.isDebugEnabled) {
        logger.debug(s"[batch] " +
          s"Adding producer transactions to commit.")
      }
      rocksWriter.putTransactions(
        producerTransactions,
        batch
      )
    }
  }

  def putConsumerTransactions(consumerTransactions: Seq[ConsumerTransactionRecord]): Unit = {
    val isFixedNow =
      isFixed.get()

    if (!isFixedNow) {
      if (logger.isDebugEnabled) {
        logger.debug(s"[batch] " +
          s"Adding consumer transactions to commit.")
      }

      rocksWriter.putConsumersCheckpoints(
        consumerTransactions,
        batch
      )
    }
  }

  def putProducerData(streamID: Int,
                      partition: Int,
                      transaction: Long,
                      data: Seq[ByteBuffer],
                      from: Int): Boolean = {
    val isFixedNow =
      isFixed.get()

    if (!isFixedNow) {
      val isDataPersisted =
        scala.util.Try {
          rocksWriter.putTransactionData(
            streamID,
            partition,
            transaction,
            data,
            from
          )
        }.getOrElse(false)

      if (logger.isDebugEnabled) {
        logger.debug(s"[batch] " +
          s"Persisting producer transactions data on stream: $streamID, partition: $partition, transaction: $transaction, " +
          s"from: $from.")
      }

      isDataPersisted
    } else {
      isFixedNow
    }
  }

  def commit(): Boolean = {
    val isFixedNow =
      isFixed.getAndSet(true)

    if (!isFixedNow) {
      batch.put(databaseIndex, key, value)
      if (batch.write()) {
        if (logger.isDebugEnabled) logger.debug(s"commit is successfully fixed.")
        true
      } else {
        false
      }
    } else {
      false
    }
  }
}
