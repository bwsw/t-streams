/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.bwsw.tstreamstransactionserver.netty.server.transactionDataService

import java.nio.ByteBuffer

import com.bwsw.tstreamstransactionserver.`implicit`.Implicits._
import com.bwsw.tstreamstransactionserver.exception.Throwable.StreamDoesNotExist
import com.bwsw.tstreamstransactionserver.netty.server.db.rocks.RocksDbConnection
import com.bwsw.tstreamstransactionserver.netty.server.streamService.{StreamKey, StreamRecord, StreamRepository}
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataService._
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.{RocksStorageOptions, StorageOptions}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

class TransactionDataService(storageOpts: StorageOptions,
                             rocksStorageOpts: RocksStorageOptions,
                             streamRepository: StreamRepository) {
  private val logger =
    LoggerFactory.getLogger(this.getClass)

  private val ttlToAdd: Int =
    rocksStorageOpts.transactionTtlAppendMs
  private val rocksDBStorageToStream =
    new java.util.concurrent.ConcurrentHashMap[StorageName, RocksDbConnection]()

  private val pathForData =
    s"${storageOpts.path}${java.io.File.separatorChar}${storageOpts.dataDirectory}${java.io.File.separatorChar}"

  private def getStorageOrCreateIt(streamRecord: StreamRecord) = {
    val key = StorageName(streamRecord.key.id.toString)
    rocksDBStorageToStream.computeIfAbsent(key, (t: StorageName) => {
      val calculatedTTL = calculateTTL(streamRecord.ttl)
      if (logger.isDebugEnabled())
        logger.debug(prefixName + s"Creating new database handler[stream: ${streamRecord.name}, ttl(in hrs): $calculatedTTL] " +
          s"for persisting and reading transactions data.")
      new RocksDbConnection(rocksStorageOpts, s"$pathForData${key.toString}", calculatedTTL)
    })
  }

  final def removeRocksDBDatabaseAndDeleteFolder(stream: Long): Unit = {
    val key = StorageName(stream.toString)
    Option(rocksDBStorageToStream.get(key)) foreach (x => x.closeAndDeleteFolder())
  }


  @throws[StreamDoesNotExist]
  final def putTransactionData(streamID: Int,
                               partition: Int,
                               transaction: Long,
                               data: Seq[ByteBuffer],
                               from: Int): Boolean = {
    if (data.isEmpty) true
    else {
      val streamRecord = streamRepository
        .get(StreamKey(streamID))
        .getOrElse(throw new StreamDoesNotExist(streamID.toString))

      val rocksDB = getStorageOrCreateIt(streamRecord)

      val batch = rocksDB.newBatch

      var currentDataID = from
      val key = Key(partition, transaction)

      data foreach { datum =>
        val sizeOfSlicedData = datum.limit() - datum.position()
        val bytes =
          if (datum.hasArray) {
            datum.array()
          }
          else {
            val bytes = new Array[Byte](sizeOfSlicedData)
            datum.get(bytes)
            bytes
          }

        val binaryKey = key.toByteArray(currentDataID)
        batch.put(binaryKey, bytes)
        if (logger.isDebugEnabled) {
          logger.debug(
            prefixName + s"On stream ${streamRecord.id}, partition: $partition, transaction $transaction " +
              s"putted data: id $currentDataID, body:$bytes"
          )
        }

        currentDataID = currentDataID + 1
      }

      val isOkay = batch.write()

      if (logger.isDebugEnabled) {
        if (isOkay)
          logger.debug(prefixName + s"On stream ${streamRecord.id}, partition: $partition, transaction $transaction " +
            s"saved transaction data successfully.")
        else
          logger.debug(prefixName + s"On stream ${streamRecord.id}, partition: $partition, transaction $transaction " +
            s"transaction data wasn't saved.")
      }

      isOkay
    }
  }

  def getTransactionData(streamID: Int,
                         partition: Int,
                         transaction: Long,
                         from: Int,
                         to: Int): Seq[ByteBuffer] = {
    val streamRecord = streamRepository
      .get(StreamKey(streamID))
      .getOrElse(throw new StreamDoesNotExist(streamID.toString))
    val rocksDB = getStorageOrThrowError(streamRecord)

    val key = Key(partition, transaction)
    val fromSeqId = key.toByteArray(from)
    val toSeqId = key.toByteArray(to)

    val iterator = rocksDB.iterator
    iterator.seek(fromSeqId)

    val data = new ArrayBuffer[ByteBuffer](to - from)
    while (iterator.isValid && ByteArray.compare(iterator.key(), toSeqId) <= 0) {
      data += java.nio.ByteBuffer.wrap(iterator.value())
      iterator.next()
    }
    iterator.close()

    if (logger.isDebugEnabled) {
      logger.debug(prefixName +
        s"Requested data on stream ${streamRecord.id}, partition: $partition, transaction $transaction " +
        s"in range [$from,$to]. Retrieved data ${data.length} units"
      )
    }

    data
  }

  @throws[StreamDoesNotExist]
  private def getStorageOrThrowError(streamRecord: StreamRecord) = {
    val key = StorageName(streamRecord.key.id.toString)


    Option(rocksDBStorageToStream.get(key))
      .getOrElse {
        if (logger.isDebugEnabled()) {
          val calculatedTTL = calculateTTL(streamRecord.ttl)
          logger.debug(prefixName +
            s"Database[stream: ${streamRecord.name}, ttl(in hrs): $calculatedTTL] doesn't exists!"
          )
        }
        throw new Exception(s"Database[stream: ${streamRecord.name}] doesn't exists!")
      }
  }

  private def calculateTTL(ttl: Long): Int = scala.math.abs((ttl + ttlToAdd).toInt)

  def closeTransactionDataDatabases(): Unit =
    rocksDBStorageToStream.values().forEach(_.close())
}

object TransactionDataService {
  val prefixName = "[TransactionDataService] "
}