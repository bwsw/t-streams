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
package com.bwsw.tstreamstransactionserver.util.multiNode

import java.io.File
import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.netty.Protocol
import com.bwsw.tstreamstransactionserver.netty.client.api.TTSInetClient
import com.bwsw.tstreamstransactionserver.util.Utils._
import com.bwsw.tstreamstransactionserver.netty.server.batch.Frame
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.data.Record
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.hierarchy.LongZookeeperTreeList
import com.bwsw.tstreamstransactionserver.rpc.{ProducerTransaction, Transaction, TransactionService}
import org.apache.bookkeeper.meta.LedgerManager
import org.apache.bookkeeper.proto.BookieServer

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.concurrent.duration._

object CommonCheckpointGroupServerTtlUtils {
  private val secondsWait = 5

  def ledgersExistInBookKeeper(ledgerManager: LedgerManager, numberOfExistingLedgers: Int): Boolean = {
    val ledgers = mutable.Set[Long]()
    val ledgerRangeIterator = ledgerManager.getLedgerRanges
    while (ledgerRangeIterator.hasNext) {
      ledgerRangeIterator.next.getLedgers.asScala.foreach(e => ledgers.add(e))
    }

    ledgers.size > numberOfExistingLedgers //there is no such time in which all ledgers closed (so size ~ size + 1 at least)
  }

  def ledgersExistInZkTree(trees: Set[LongZookeeperTreeList], numberOfExistingLedgers: Int): Boolean = {
    val nodes = mutable.Set[Long]()

    trees.foreach(tree => {
      addNodes(tree.firstEntityId, nodes, tree)
    })

    nodes.size > numberOfExistingLedgers
  }

  def addNodes(node: Option[Long], nodes: mutable.Set[Long], tree: LongZookeeperTreeList): Unit = {
    node match {
      case Some(id) =>
        val nextNode = tree.getNextNode(id)
        nodes.add(id)
        addNodes(nextNode, nodes, tree)
      case None => //do nothing
    }
  }

  def toMs(seconds: Int): Int = TimeUnit.SECONDS.toMillis(seconds).toInt

  def fillEntryLog(client: TTSInetClient, times: Int, entryLogSizeLimit: Int): Int = {
    var totalWaitingTime = System.currentTimeMillis()
    val stream = getRandomStream
    val streamId = Await.result(client.putStream(stream), secondsWait.seconds)
    val producerTransactions = Array.fill(500)(getRandomProducerTransaction(streamId, stream))
    val size = getSize(producerTransactions)
    val numberOfEntries = (entryLogSizeLimit / size + 2) * times
    (0 until numberOfEntries).foreach(_ =>
      Await.result(client.putTransactions(producerTransactions, Seq()), secondsWait.seconds)
    )
    totalWaitingTime = (System.currentTimeMillis() - totalWaitingTime) / 1000

    totalWaitingTime.toInt
  }

  def entryLogsExistInBookKeeper(bookieServers: Array[BookieServer], numberOfFiles: Int): Boolean = {
    bookieServers.flatMap(_.getBookie.getLedgerDirsManager.getAllLedgerDirs.asScala).forall(ledgerDirectory => {
      hasLogFiles(ledgerDirectory, numberOfFiles)
    })
  }

  def hasLogFiles(ledgerDirectory: File, numberOfFiles: Int): Boolean = {
    var currentNumberOfFiles = 0
    ledgerDirectory.listFiles().foreach(file => {
      if (file.isFile) {
        val name: String = file.getName
        if (name.endsWith(".log")) {
          currentNumberOfFiles += 1
        }
      }
    })

    currentNumberOfFiles > numberOfFiles //there is no such time in which all entry logs closed (so size ~ size + 1 at least)
  }

  def getSize(txns: Array[ProducerTransaction]): Int = {
    val transactions = txns.map(txn => Transaction(Some(txn), None))
    val request = TransactionService.PutTransactions.Args(transactions)
    val message = Protocol.PutTransactions.encodeRequestToMessage(request)(1L, 1, isFireAndForgetMethod = false)
    val record = new Record(Frame.PutTransactionsType, System.currentTimeMillis(), 1, message.body).toByteArray

    record.length
  }
}
