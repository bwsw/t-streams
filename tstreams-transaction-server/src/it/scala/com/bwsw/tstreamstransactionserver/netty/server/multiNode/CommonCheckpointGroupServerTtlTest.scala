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
package com.bwsw.tstreamstransactionserver.netty.server.multiNode

import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.netty.client.ClientBuilder
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.hierarchy.LongZookeeperTreeList
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.BookkeeperOptions
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.StorageOptions
import com.bwsw.tstreamstransactionserver.util.Utils
import com.bwsw.tstreamstransactionserver.util.multiNode.{Util, ZkServerTxnMultiNodeServerTxnClient}
import org.apache.bookkeeper.meta.LedgerManagerFactory
import org.apache.bookkeeper.zookeeper.ZooKeeperClient
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable

class CommonCheckpointGroupServerTtlTest
  extends FlatSpec
    with BeforeAndAfterAll
    with Matchers {

  private val ensembleNumber = 3
  private val writeQuorumNumber = 3
  private val ackQuorumNumber = 2

  private val treeFactor = 2
  private val waitMs = 200
  private val maxIdleTimeBetweenRecords = 1
  private val dataCompactionInterval = maxIdleTimeBetweenRecords * 3
  private val ttl = dataCompactionInterval * 2

  private val bookkeeperOptions = BookkeeperOptions(
    ensembleNumber,
    writeQuorumNumber,
    ackQuorumNumber,
    "test".getBytes(),
    ttl)

  private lazy val serverBuilder = new CommonCheckpointGroupServerBuilder()
    .withServerStorageOptions(StorageOptions(dataCompactionInterval = dataCompactionInterval))
  private lazy val clientBuilder = new ClientBuilder()

  private val bookiesNumber =
    ensembleNumber max writeQuorumNumber max ackQuorumNumber

  private lazy val (zkServer, zkClient, bookieServers) =
    Utils.startZkAndBookieServerWithConfig(bookiesNumber, waitMs)

  private lazy val zk = ZooKeeperClient.newBuilder.connectString(zkClient.getZookeeperClient.getCurrentConnectionString).build
  //doesn't matter which one's conf because zk is a common part
  private lazy val ledgerManagerFactory = LedgerManagerFactory.newLedgerManagerFactory(bookieServers.head._2, zk)
  private lazy val ledgerManager = ledgerManagerFactory.newLedgerManager

  override def beforeAll(): Unit = {
    zkServer
    zkClient
    bookieServers
    ledgerManager
  }

  override def afterAll(): Unit = {
    ledgerManager.close()
    bookieServers.foreach(_._1.shutdown())
    zkClient.close()
    zkServer.close()
  }

  "Expired ledgers" should "be deleted according to settings if a server works in a stable way" in {
    val bundle: ZkServerTxnMultiNodeServerTxnClient = Util.getCommonCheckpointGroupServerBundle(
      zkClient, bookkeeperOptions, serverBuilder, clientBuilder, toMs(maxIdleTimeBetweenRecords)
    )
    val cgPath = bundle.serverBuilder.getCommonPrefixesOptions.checkpointGroupPrefixesOptions.checkpointGroupZkTreeListPrefix
    val cgTree = new LongZookeeperTreeList(zkClient, cgPath)

    val commonPath = bundle.serverBuilder.getCommonPrefixesOptions.commonMasterZkTreeListPrefix
    val commonTree = new LongZookeeperTreeList(zkClient, commonPath)

    val trees = Set(cgTree, commonTree)

    bundle.operate(x => {
      Thread.sleep(toMs(dataCompactionInterval) + waitMs)

      val createdLedgers = (dataCompactionInterval / maxIdleTimeBetweenRecords) * treeFactor
      ledgersExistInBookKeeper(createdLedgers) shouldBe true //because we use CommonCheckpointGroupServer
      // that has two zk trees so creates two times more ledgers
      ledgersExistInZkTree(trees, createdLedgers) shouldBe true

      Thread.sleep(toMs(ttl) + waitMs)

      ledgersExistInBookKeeper((ttl / maxIdleTimeBetweenRecords) * treeFactor + createdLedgers) shouldBe false
      ledgersExistInZkTree(trees, (ttl / maxIdleTimeBetweenRecords) * treeFactor + createdLedgers) shouldBe false
    })
  }

  import scala.collection.JavaConverters._

  private def ledgersExistInBookKeeper(numberOfExistingLedgers: Int): Boolean = {
    val ledgers = mutable.Set[Long]()
    val ledgerRangeIterator = ledgerManager.getLedgerRanges
    while (ledgerRangeIterator.hasNext) {
      ledgerRangeIterator.next.getLedgers.asScala.foreach(e => ledgers.add(e))
    }

    ledgers.size > numberOfExistingLedgers //there is no such time in which all ledgers closed (so size ~ size + 1 at least)
  }

  private def ledgersExistInZkTree(trees: Set[LongZookeeperTreeList], numberOfExistingLedgers: Int): Boolean = {
    val nodes = mutable.Set[Long]()

    trees.foreach(tree => {
      addNodes(tree.firstEntityId, nodes, tree)
    })

    nodes.size > numberOfExistingLedgers
  }

  private def addNodes(node: Option[Long], nodes: mutable.Set[Long], tree: LongZookeeperTreeList): Unit = {
    node match {
      case Some(id) =>
        val nextNode = tree.getNextNode(id)
        nodes.add(id)
        addNodes(nextNode, nodes, tree)
      case None => //do nothing
    }
  }

  private def toMs(seconds: Int): Int = TimeUnit.SECONDS.toMillis(seconds).toInt
}

