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

import javax.naming.TimeLimitExceededException

import com.bwsw.tstreamstransactionserver.netty.client.ClientBuilder
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.hierarchy.LongZookeeperTreeList
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.BookkeeperOptions
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.StorageOptions
import com.bwsw.tstreamstransactionserver.util.Utils.{uuid, _}
import com.bwsw.tstreamstransactionserver.util.multiNode.CommonCheckpointGroupServerTtlUtils._
import com.bwsw.tstreamstransactionserver.util.multiNode.MultiNodeUtils._
import org.apache.bookkeeper.conf.ServerConfiguration
import org.apache.bookkeeper.meta.{LedgerManager, LedgerManagerFactory}
import org.apache.bookkeeper.proto.BookieServer
import org.apache.bookkeeper.zookeeper.ZooKeeperClient
import org.apache.curator.framework.CuratorFramework
import org.scalatest.{Matchers, Outcome, fixture}

import scala.util.{Failure, Success, Try}

class MasterChangedCommonCheckpointGroupServerTtlTest extends fixture.FlatSpec with Matchers {

  private val ensembleNumber = 3
  private val writeQuorumNumber = 3
  private val ackQuorumNumber = 2

  /**
    * because we use CommonCheckpointGroupServer that has two zk trees so creates two times more ledgers
    */
  private val treeFactor = 2
  private val gcWaitTimeMs = 500
  private val entryLogSizeLimit = 1024 * 1024
  private val maxIdleTimeBetweenRecords = 1
  private val dataCompactionInterval = maxIdleTimeBetweenRecords * 2
  private val ttl = dataCompactionInterval * 2
  private val timeToWaitEntitiesDeletion = ttl + dataCompactionInterval * treeFactor

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

  case class FixtureParam(zkClient: CuratorFramework,
                          bookieServers: Array[(BookieServer, ServerConfiguration)],
                          ledgerManager: LedgerManager)

  def withFixture(test: OneArgTest): Outcome = {
    val (zkServer, zkClient, bookieServers) =
      startZkAndBookieServerWithConfig(bookiesNumber, gcWaitTimeMs, entryLogSizeLimit)

    val zk = ZooKeeperClient.newBuilder.connectString(zkClient.getZookeeperClient.getCurrentConnectionString).build
    //doesn't matter which one's conf because zk is a common part
    val ledgerManagerFactory = LedgerManagerFactory.newLedgerManagerFactory(bookieServers.head._2, zk)
    val ledgerManager = ledgerManagerFactory.newLedgerManager

    val fixtureParam = FixtureParam(zkClient, bookieServers, ledgerManager)

    Try {
      withFixture(test.toNoArgTest(fixtureParam))
    } match {
      case Success(x) =>
        ledgerManager.close()
        bookieServers.foreach(_._1.shutdown())
        zkClient.close()
        zkServer.close()
        x
      case Failure(e: Throwable) =>
        ledgerManager.close()
        bookieServers.foreach(_._1.shutdown())
        zkClient.close()
        zkServer.close()
        throw e
    }
  }

  "Expired ledgers and bk entry logs" should "be deleted according to settings if a master changed" in { fixture =>
    val firstServerNumber = 0
    val secondServerNumber = 1
    val cluster = getCommonCheckpointGroupCluster(
      fixture.zkClient, bookkeeperOptions, serverBuilder, clientBuilder, clusterSize = 2, toMs(maxIdleTimeBetweenRecords))

    val cgPath = cluster.serverBuilder.getCommonPrefixesOptions.checkpointGroupPrefixesOptions.checkpointGroupZkTreeListPrefix
    val cgTree = new LongZookeeperTreeList(fixture.zkClient, cgPath)

    val commonPath = cluster.serverBuilder.getCommonPrefixesOptions.commonMasterZkTreeListPrefix
    val commonTree = new LongZookeeperTreeList(fixture.zkClient, commonPath)

    val trees = Set(cgTree, commonTree)

    cluster.start(firstServerNumber)
    cluster.startClient()
    cluster.operate(() => {
      //create the required number of txns to roll over the initial entryLog (0.log)
      var createdLedgers = 0
      val numberOfEntryLogs = 2
      val waitingTime = fillEntryLog(cluster.client, numberOfEntryLogs, entryLogSizeLimit)

      if (waitingTime < dataCompactionInterval) {
        Thread.sleep(toMs(dataCompactionInterval))
        createdLedgers = (dataCompactionInterval + waitingTime) / maxIdleTimeBetweenRecords * treeFactor
      } else if (waitingTime < ttl) {
        createdLedgers = waitingTime / maxIdleTimeBetweenRecords * treeFactor
      } else throw new TimeLimitExceededException("Decrease time of entry logs creation " +
        "because entry logs files will be deleted if the creation time is greater than ttl")

      ledgersExistInZkTree(trees, createdLedgers) shouldBe true
      ledgersExistInBookKeeper(fixture.ledgerManager, createdLedgers) shouldBe true
      entryLogsExistInBookKeeper(fixture.bookieServers.map(_._1), numberOfEntryLogs) shouldBe true

      cluster.start(secondServerNumber)
      cluster.stop(firstServerNumber)

      Thread.sleep(toMs(timeToWaitEntitiesDeletion) + gcWaitTimeMs)

      val secondPartOfCreatedLedgers = timeToWaitEntitiesDeletion / maxIdleTimeBetweenRecords * treeFactor
      ledgersExistInZkTree(trees, secondPartOfCreatedLedgers + createdLedgers) shouldBe false
      ledgersExistInBookKeeper(fixture.ledgerManager, secondPartOfCreatedLedgers + createdLedgers) shouldBe false
      entryLogsExistInBookKeeper(fixture.bookieServers.map(_._1), numberOfEntryLogs) shouldBe false
    })
  }
}