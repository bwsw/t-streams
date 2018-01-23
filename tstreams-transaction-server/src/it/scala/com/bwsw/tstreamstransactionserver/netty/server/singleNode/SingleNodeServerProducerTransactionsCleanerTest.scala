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

package com.bwsw.tstreamstransactionserver.netty.server.singleNode

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.ProducerTransactionRecord
import com.bwsw.tstreamstransactionserver.rpc.{ProducerTransaction, StreamValue, TransactionStates}
import com.bwsw.tstreamstransactionserver.util
import com.bwsw.tstreamstransactionserver.util.Utils.{getRandomStream, startZkServerAndGetIt}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.util.Random


class SingleNodeServerProducerTransactionsCleanerTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll {

  private val txnCounter = new AtomicLong(0)

  private def getRandomProducerTransaction(streamID: Int,
                                           streamObj: StreamValue,
                                           ttlTxn: Long) = ProducerTransaction(
    stream = streamID,
    partition = streamObj.partitions,
    transactionID = txnCounter.getAndIncrement(),
    state = TransactionStates.Opened,
    quantity = -1,
    ttl = ttlTxn
  )

  private lazy val (zkServer, zkClient) =
    startZkServerAndGetIt

  override def beforeAll(): Unit = {
    zkServer
    zkClient
  }

  override def afterAll(): Unit = {
    zkClient.close()
    zkServer.close()
  }

  "Cleaner" should "remove all expired transactions from OpenedTransactions table and invalidate them in AllTransactions table" in {
    val bundle = util.Utils.getRocksReaderAndRocksWriter(zkClient)

    val streamService = bundle.streamService
    val rocksReader = bundle.rocksReader
    val rocksWriter = bundle.rocksWriter

    val maxTTLForProducerTransactionSec = 5
    val producerTxnNumber = 100

    def ttlSec = TimeUnit.SECONDS.toMillis(Random.nextInt(maxTTLForProducerTransactionSec))

    val stream = getRandomStream

    val streamID = streamService.putStream(stream.name, stream.partitions, stream.description, stream.ttl)

    val currentTime = System.currentTimeMillis()
    val producerTransactionsWithTimestamp: Array[(ProducerTransaction, Long)] = Array.fill(producerTxnNumber) {
      val producerTransaction = getRandomProducerTransaction(streamID, stream, ttlSec)
      (producerTransaction, System.currentTimeMillis())
    }
    val minTransactionID = producerTransactionsWithTimestamp.minBy(_._1.transactionID)._1.transactionID
    val maxTransactionID = producerTransactionsWithTimestamp.maxBy(_._1.transactionID)._1.transactionID

    val transactionsWithTimestamp = producerTransactionsWithTimestamp.map {
      case (producerTxn, timestamp) => ProducerTransactionRecord(producerTxn, timestamp)
    }

    val batch = bundle.newBatch
    rocksWriter.putTransactions(transactionsWithTimestamp, batch)
    batch.write()

    rocksWriter.createAndExecuteTransactionsToDeleteTask(
      currentTime + TimeUnit.SECONDS.toMillis(maxTTLForProducerTransactionSec)
    )

    val expiredTransactions = producerTransactionsWithTimestamp.map { case (producerTxn, _) =>
      ProducerTransaction(
        producerTxn.stream,
        producerTxn.partition,
        producerTxn.transactionID,
        TransactionStates.Invalid,
        0,
        0L
      )
    }

    rocksReader.scanTransactions(
      streamID,
      stream.partitions,
      minTransactionID,
      maxTransactionID,
      Int.MaxValue,
      Set(TransactionStates.Opened)
    ).producerTransactions should contain theSameElementsAs expiredTransactions

    bundle.closeDBAndDeleteFolder()
  }

}
