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

package com.bwsw.tstreamstransactionserver

import java.io.File

import com.bwsw.commitlog.filesystem.CommitLogBinary
import com.bwsw.tstreamstransactionserver.netty.Protocol
import com.bwsw.tstreamstransactionserver.netty.server.batch.Frame
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService.{FileKey, FileValue}
import org.apache.commons.io.FileUtils
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
import org.rocksdb._

import scala.collection.mutable.ArrayBuffer

object DumpProcessedCommitLogUtility {
  RocksDB.loadLibrary()

  sealed trait Transaction
  case class ProducerTransaction(stream: Int, partition: Int, transactionID: Long, state: String, ttl: Long) extends Transaction
  case class ConsumerTransaction(stream: Int, partition: Int, transactionID: Long, name: String) extends Transaction
  case class CommitLogRecord(timestamp: Long, transactions: Seq[Transaction])
  case class CommitLogFile(id: Long, md5: Option[Seq[Byte]], transactions: Seq[CommitLogRecord])

  private implicit val formatsTransaction = Serialization.formats(ShortTypeHints(List(classOf[Transaction])))

  def main(args: Array[String]): Unit = {
    if (args.isEmpty) throw new IllegalArgumentException("Path to rocksdb folder should be provided.")
    else process(args(0))
  }

  def process(path: String): Unit = {
    val options: Options = new Options()
    val file = new File(path)
    val client = {
      FileUtils.forceMkdir(file)
      TtlDB.open(options, file.getAbsolutePath, -1, true)
    }

    val iterator = client.newIterator()
    iterator.seekToFirst()
    while (iterator.isValid) {
      val key = iterator.key()
      val value = iterator.value()
      val json = pretty(render(Extraction.decompose(RecordToProducerOrConsumerTransaction(key, value))))
      println(json)
      iterator.next()
    }
    iterator.close()
    client.close()
  }

  private def deserializePutTransaction(message: Array[Byte]) =
    Protocol.PutTransaction.decodeRequest(message)
  private def deserializePutTransactions(message: Array[Byte]) =
    Protocol.PutTransactions.decodeRequest(message)
  private def deserializeSetConsumerState(message: Array[Byte]) =
    Protocol.PutConsumerCheckpoint.decodeRequest(message)

  private def retrieveTransactions(record: com.bwsw.commitlog.CommitLogRecord): Seq[(com.bwsw.tstreamstransactionserver.rpc.Transaction, Long)] =
    Frame(record.messageType.toInt) match {
      case Frame.PutTransactionType =>
        val txn = deserializePutTransaction(record.message)
        Seq((txn.transaction, record.timestamp))
      case Frame.PutTransactionsType =>
        val txns = deserializePutTransactions(record.message)
        txns.transactions.map(txn => (txn, record.timestamp))
      case Frame.PutConsumerCheckpointType =>
        val args = deserializeSetConsumerState(record.message)
        val consumerTransaction = com.bwsw.tstreamstransactionserver.rpc.ConsumerTransaction(args.streamID, args.partition, args.transaction, args.name)
        Seq((com.bwsw.tstreamstransactionserver.rpc.Transaction(None, Some(consumerTransaction)), record.timestamp))
      case _ => throw new IllegalArgumentException("Undefined method type for retrieving message from commit log record")
    }


  def RecordToProducerOrConsumerTransaction(key: Array[Byte], value:Array[Byte]): CommitLogFile = {
    val fileKey = FileKey.fromByteArray(key)
    val fileValue = FileValue.fromByteArray(value)
    val binaryFile = new CommitLogBinary(fileKey.id, fileValue.fileContent, fileValue.fileMD5Content)

    val recordsBuffer = new ArrayBuffer[CommitLogRecord]()
    val iterator = binaryFile.getIterator
    while (iterator.hasNext()) {
      iterator.next().right.foreach{record =>
        val transactions = retrieveTransactions(record)
        val transactionsJson = transactions.map { transaction =>
          val txn = transaction._1
          val transactionJson: Transaction = if (txn._1.isDefined) {
            txn._1.map(x => ProducerTransaction(x.stream, x.partition, x.transactionID, x.state.toString, x.ttl)).get
          } else {
            txn._2.map(x => ConsumerTransaction(x.stream, x.partition, x.transactionID, x.name)).get
          }
          transactionJson
        }
        recordsBuffer += CommitLogRecord(record.timestamp, transactionsJson)
      }
    }
    CommitLogFile(fileKey.id, fileValue.fileMD5Content.map(_.toSeq), recordsBuffer)
  }
}
