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

package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy

import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreamstransactionserver.netty.Protocol
import com.bwsw.tstreamstransactionserver.netty.server.authService.AuthService
import com.bwsw.tstreamstransactionserver.netty.server.batch.Frame
import com.bwsw.tstreamstransactionserver.netty.server.consumerService.{ConsumerTransactionKey, ConsumerTransactionRecord}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.data.Record
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.hierarchy.{BookkeeperToRocksWriter, LongNodeCache, LongZookeeperTreeList, ZkMultipleTreeListReader}
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates.{Checkpointed, Opened}
import com.bwsw.tstreamstransactionserver.rpc._
import com.bwsw.tstreamstransactionserver.util.Utils.uuid
import com.bwsw.tstreamstransactionserver.util.multiNode.MultiNudeUtils
import com.bwsw.tstreamstransactionserver.util.{Utils, multiNode}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable
import scala.util.Random

class BookkeeperToRocksWriterTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll {

  private val streamIDGen = new java.util.concurrent.atomic.AtomicInteger(0)
  private val partitionsNumber = 100
  private val token = 846487864

  private def generateStream =
    Stream(
      id = streamIDGen.getAndIncrement(),
      name = Random.nextString(10),
      partitions = partitionsNumber,
      None,
      Long.MaxValue,
      ""
    )

  private def buildProducerTransaction(streamID: Int,
                                       partition: Int,
                                       state: TransactionStates,
                                       txnID: Long,
                                       ttlTxn: Long) =
    ProducerTransaction(
      stream = streamID,
      partition = partition,
      transactionID = txnID,
      state = state,
      quantity = -1,
      ttl = ttlTxn
    )

  private def buildConsumerTransaction(streamID: Int,
                                       partition: Int,
                                       txnID: Long,
                                       name: String) =
    ConsumerTransaction(
      streamID,
      partition,
      txnID,
      name
    )


  private def genProducerTransactionsWrappedInRecords(transactionIDGen: AtomicLong,
                                                      transactionNumber: Int,
                                                      streamID: Int,
                                                      partition: Int,
                                                      state: TransactionStates,
                                                      ttlTxn: Long,
                                                      token: Int = AuthService.UnauthenticatedToken) = {
    (0 until transactionNumber)
      .map(txnID => buildProducerTransaction(
        streamID,
        partition,
        state,
        txnID,
        ttlTxn
      ))
      .map { txn =>
        val binaryTransaction = Protocol.PutTransaction.encodeRequest(
          TransactionService.PutTransaction.Args(Transaction(Some(txn), None))
        )
        new Record(
          Frame.PutTransactionType,
          transactionIDGen.getAndIncrement(),
          token,
          binaryTransaction
        )
      }
  }

  private def genConsumerTransactionsWrappedInRecords(consumerMap: mutable.Map[ConsumerTransactionKey, ConsumerTransactionRecord],
                                                      transactionIDGen: AtomicLong,
                                                      transactionNumber: Int,
                                                      streamID: Int,
                                                      partition: Int,
                                                      name: String) = {
    (0 until transactionNumber)
      .map { txnID =>
        val consumerTransaction = buildConsumerTransaction(
          streamID,
          partition,
          txnID,
          name)

        val binaryTransaction = Protocol.PutTransaction.encodeRequest(
          TransactionService.PutTransaction.Args(Transaction(None, Some(consumerTransaction)))
        )

        val record = new Record(
          Frame.PutTransactionType,
          transactionIDGen.getAndIncrement(),
          token,
          binaryTransaction
        )

        val binaryTransactionRecord =
          ConsumerTransactionRecord(
            consumerTransaction,
            record.timestamp
          )

        val update = consumerMap.get(binaryTransactionRecord.key)
          .map(txn =>
            if (txn.timestamp < binaryTransactionRecord.timestamp)
              binaryTransactionRecord
            else
              txn
          )
          .getOrElse(binaryTransactionRecord)

        consumerMap.put(update.key, update)

        record
      }
  }

  private lazy val (zkServer, zkClient) = Utils.startZkServerAndGetIt

  override def beforeAll(): Unit = {
    zkServer
    zkClient
  }

  override def afterAll(): Unit = {
    zkClient.close()
    zkServer.close()
  }

  it should "return opened and checkpointed transactions and process entirely 2 ledgers as they are opened at the same time and closed too" in {
    val stream = generateStream
    val partition = 1

    val producerTransactionsNumber = 99

    val initialTime = 0L
    val atomicLong = new AtomicLong(initialTime)

    val firstTreeRecords =
      genProducerTransactionsWrappedInRecords(
        atomicLong,
        producerTransactionsNumber,
        stream.id,
        partition,
        Opened,
        50000L
      )

    val firstTimestamp =
      atomicLong.getAndIncrement()


    atomicLong.set(initialTime)

    val secondTreeRecords =
      genProducerTransactionsWrappedInRecords(
        atomicLong,
        producerTransactionsNumber,
        stream.id,
        partition,
        Checkpointed,
        50000L
      )

    val secondTimestamp =
      atomicLong.getAndIncrement()


    val storage = new LedgerManagerInMemory

    val firstLedger = storage.createLedger(firstTimestamp)
    firstTreeRecords.foreach(record => firstLedger.addRecord(record))

    val secondLedger = storage.createLedger(secondTimestamp)
    secondTreeRecords.foreach(record => secondLedger.addRecord(record))

    val zkTreeList1 = new LongZookeeperTreeList(zkClient, s"/$uuid")
    val zkTreeList2 = new LongZookeeperTreeList(zkClient, s"/$uuid")

    zkTreeList1.createNode(firstLedger.id)
    zkTreeList2.createNode(secondLedger.id)


    val zkTreeListLastClosedLedgerPrefix1 =
      s"/$uuid"
    zkClient
      .create()
      .creatingParentsIfNeeded()
      .forPath(
        zkTreeListLastClosedLedgerPrefix1,
        java.nio.ByteBuffer.allocate(8).putLong(firstLedger.id).array()
      )

    val zkTreeListLastClosedLedgerPrefix2 =
      s"/$uuid"
    zkClient
      .create()
      .creatingParentsIfNeeded()
      .forPath(
        zkTreeListLastClosedLedgerPrefix2,
        java.nio.ByteBuffer.allocate(8).putLong(secondLedger.id).array()
      )

    val commonMasterLastClosedLedger =
      new LongNodeCache(
        zkClient,
        zkTreeListLastClosedLedgerPrefix1
      )

    val checkpointMasterLastClosedLedger =
      new LongNodeCache(
        zkClient,
        zkTreeListLastClosedLedgerPrefix2
      )

    val lastClosedLedgerHandlers =
      Array(commonMasterLastClosedLedger, checkpointMasterLastClosedLedger)
    lastClosedLedgerHandlers.foreach(_.startMonitor())

    val trees = Array(zkTreeList1, zkTreeList2)
    val testReader = new ZkMultipleTreeListReader(
      trees,
      lastClosedLedgerHandlers,
      storage
    )

    val bundle = MultiNudeUtils.getTransactionServerBundle(zkClient)

    bundle.operate { transactionServer =>

      val commitLogService =
        bundle.multiNodeCommitLogService

      val bookkeeperToRocksWriter =
        new BookkeeperToRocksWriter(
          testReader,
          commitLogService,
          bundle.rocksWriter
        )

      bookkeeperToRocksWriter.processAndPersistRecords()

      val result = transactionServer.scanTransactions(
        stream.id,
        partition,
        initialTime,
        atomicLong.get(),
        Int.MaxValue,
        Set(TransactionStates.Opened)
      )

      val processedLedgerAndRecord1 = commitLogService.getLastProcessedLedgersAndRecordIDs


      result.producerTransactions.length shouldBe producerTransactionsNumber
      result.producerTransactions.forall(_.state == TransactionStates.Checkpointed) shouldBe true
      result.producerTransactions.last.transactionID shouldBe producerTransactionsNumber - 1L

      bookkeeperToRocksWriter.processAndPersistRecords()

      val processedLedgerAndRecord2 = commitLogService.getLastProcessedLedgersAndRecordIDs


      processedLedgerAndRecord1 should contain theSameElementsInOrderAs processedLedgerAndRecord2
    }
  }


  it should "return checkpointed transactions and process entirely 1-st ledger records and half of records of 2-nd ledgers" in {
    val token = Random.nextInt()
    val stream = generateStream
    val partition = 1

    val producerTransactionsNumber = 99

    val initialTime = 0L
    val atomicLong = new AtomicLong(initialTime)
    val firstTimestamp =
      atomicLong.get()

    val firstTreeRecords = new Record(Frame.TokenCreatedType, firstTimestamp, token, Array.emptyByteArray) +:
      genProducerTransactionsWrappedInRecords(
        atomicLong,
        producerTransactionsNumber,
        stream.id,
        partition,
        Opened,
        50000L,
        token
      )


    val offset = 50
    atomicLong.set(initialTime + offset)
    val secondTimestamp =
      atomicLong.get()


    val secondTreeRecords =
      genProducerTransactionsWrappedInRecords(
        atomicLong,
        producerTransactionsNumber,
        stream.id,
        partition,
        Checkpointed,
        50000L,
        token
      )

    val storage = new LedgerManagerInMemory

    val firstLedger = storage.createLedger(firstTimestamp)
    firstTreeRecords.foreach(record => firstLedger.addRecord(record))

    val secondLedger = storage.createLedger(secondTimestamp)
    secondTreeRecords.foreach(record => secondLedger.addRecord(record))

    val zkTreeList1 = new LongZookeeperTreeList(zkClient, s"/$uuid")
    val zkTreeList2 = new LongZookeeperTreeList(zkClient, s"/$uuid")

    zkTreeList1.createNode(firstLedger.id)
    zkTreeList2.createNode(secondLedger.id)

    val zkTreeListLastClosedLedgerPrefix1 =
      s"/$uuid"
    zkClient
      .create()
      .creatingParentsIfNeeded()
      .forPath(
        zkTreeListLastClosedLedgerPrefix1,
        java.nio.ByteBuffer.allocate(8).putLong(firstLedger.id).array()
      )

    val zkTreeListLastClosedLedgerPrefix2 =
      s"/$uuid"
    zkClient
      .create()
      .creatingParentsIfNeeded()
      .forPath(
        zkTreeListLastClosedLedgerPrefix2,
        java.nio.ByteBuffer.allocate(8).putLong(secondLedger.id).array()
      )

    val commonMasterLastClosedLedger =
      new LongNodeCache(
        zkClient,
        zkTreeListLastClosedLedgerPrefix1
      )

    val checkpointMasterLastClosedLedger =
      new LongNodeCache(
        zkClient,
        zkTreeListLastClosedLedgerPrefix2
      )

    val lastClosedLedgerHandlers =
      Array(commonMasterLastClosedLedger, checkpointMasterLastClosedLedger)
    lastClosedLedgerHandlers.foreach(_.startMonitor())


    val trees = Array(zkTreeList1, zkTreeList2)
    val testReader = new ZkMultipleTreeListReader(
      trees,
      lastClosedLedgerHandlers,
      storage
    )

    val bundle = MultiNudeUtils.getTransactionServerBundle(zkClient)

    bundle.operate { transactionServer =>

      val commitLogService =
        bundle.multiNodeCommitLogService

      val bookkeeperToRocksWriter =
        new BookkeeperToRocksWriter(
          testReader,
          commitLogService,
          bundle.rocksWriter
        )

      bookkeeperToRocksWriter.processAndPersistRecords()

      val result = transactionServer.scanTransactions(
        stream.id,
        partition,
        initialTime,
        atomicLong.get(),
        Int.MaxValue,
        Set()
      )

      val processedLedgerAndRecord1 = commitLogService.getLastProcessedLedgersAndRecordIDs


      result.producerTransactions.length shouldBe producerTransactionsNumber
      result.producerTransactions.take(offset - 1).forall(_.state == TransactionStates.Checkpointed) shouldBe true
      result.producerTransactions.takeRight(offset).forall(_.state == TransactionStates.Opened) shouldBe true
      result.producerTransactions.last.transactionID shouldBe producerTransactionsNumber - 1L

      bookkeeperToRocksWriter.processAndPersistRecords()

      val processedLedgerAndRecord2 = commitLogService.getLastProcessedLedgersAndRecordIDs

      processedLedgerAndRecord1 should contain theSameElementsInOrderAs processedLedgerAndRecord2
    }
  }


  it should "return consumer transactions properly" in {
    val stream = generateStream
    val partition = 1

    val producerTransactionsNumber = 99

    val initialTime = 0L
    val atomicLong = new AtomicLong(initialTime)


    val streamsNames =
      Array.fill(producerTransactionsNumber)(uuid)

    val consumerTransactionRecords =
      mutable.Map[ConsumerTransactionKey, ConsumerTransactionRecord]()

    val firstTreeRecords =
      genConsumerTransactionsWrappedInRecords(
        consumerTransactionRecords,
        atomicLong,
        producerTransactionsNumber,
        stream.id,
        partition,
        streamsNames(Random.nextInt(producerTransactionsNumber))
      )

    val firstTimestamp =
      atomicLong.getAndIncrement()


    atomicLong.set(initialTime)


    val secondTreeRecords =
      genConsumerTransactionsWrappedInRecords(
        consumerTransactionRecords,
        atomicLong,
        producerTransactionsNumber,
        stream.id,
        partition,
        streamsNames(Random.nextInt(producerTransactionsNumber))
      )

    val secondTimestamp =
      atomicLong.getAndIncrement()


    val storage = new LedgerManagerInMemory

    val firstLedger = storage.createLedger(firstTimestamp)
    firstTreeRecords.foreach(record => firstLedger.addRecord(record))

    val secondLedger = storage.createLedger(secondTimestamp)
    secondTreeRecords.foreach(record => secondLedger.addRecord(record))

    val zkTreeList1 = new LongZookeeperTreeList(zkClient, s"/$uuid")
    val zkTreeList2 = new LongZookeeperTreeList(zkClient, s"/$uuid")

    zkTreeList1.createNode(firstLedger.id)
    zkTreeList2.createNode(secondLedger.id)


    val zkTreeListLastClosedLedgerPrefix1 =
      s"/$uuid"
    zkClient
      .create()
      .creatingParentsIfNeeded()
      .forPath(
        zkTreeListLastClosedLedgerPrefix1,
        java.nio.ByteBuffer.allocate(8).putLong(firstLedger.id).array()
      )

    val zkTreeListLastClosedLedgerPrefix2 =
      s"/$uuid"
    zkClient
      .create()
      .creatingParentsIfNeeded()
      .forPath(
        zkTreeListLastClosedLedgerPrefix2,
        java.nio.ByteBuffer.allocate(8).putLong(secondLedger.id).array()
      )

    val commonMasterLastClosedLedger =
      new LongNodeCache(
        zkClient,
        zkTreeListLastClosedLedgerPrefix1
      )

    val checkpointMasterLastClosedLedger =
      new LongNodeCache(
        zkClient,
        zkTreeListLastClosedLedgerPrefix2
      )

    val lastClosedLedgerHandlers =
      Array(commonMasterLastClosedLedger, checkpointMasterLastClosedLedger)
    lastClosedLedgerHandlers.foreach(_.startMonitor())


    val trees = Array(zkTreeList1, zkTreeList2)
    val testReader = new ZkMultipleTreeListReader(
      trees,
      lastClosedLedgerHandlers,
      storage
    )
    val bundle = MultiNudeUtils.getTransactionServerBundle(zkClient)

    bundle.operate { transactionServer =>

      val commitLogService =
        bundle.multiNodeCommitLogService

      val bookkeeperToRocksWriter =
        new BookkeeperToRocksWriter(
          testReader,
          commitLogService,
          bundle.rocksWriter
        )

      bookkeeperToRocksWriter.processAndPersistRecords()

      val processedLedgerAndRecord1 = commitLogService.getLastProcessedLedgersAndRecordIDs

      consumerTransactionRecords.foreach {
        case (consumerKey, consumerValue) =>
          transactionServer.getConsumerState(
            consumerKey.name,
            consumerKey.streamID,
            consumerKey.partition
          ) shouldBe consumerValue.transactionID
      }

      bookkeeperToRocksWriter.processAndPersistRecords()

      val processedLedgerAndRecord2 = commitLogService.getLastProcessedLedgersAndRecordIDs

      processedLedgerAndRecord1 should contain theSameElementsInOrderAs processedLedgerAndRecord2
    }
  }
}
