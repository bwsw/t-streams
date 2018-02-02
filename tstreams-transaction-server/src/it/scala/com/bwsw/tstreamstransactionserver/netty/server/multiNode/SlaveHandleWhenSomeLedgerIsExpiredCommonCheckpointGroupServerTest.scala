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

import java.util.concurrent.{CountDownLatch, TimeUnit}
import javax.naming.TimeLimitExceededException

import com.bwsw.tstreamstransactionserver.exception.Throwable.ReadOperationIsNotAllowedException
import com.bwsw.tstreamstransactionserver.netty.SocketHostPortPair
import com.bwsw.tstreamstransactionserver.netty.client.zk.ZKMasterPathMonitor
import com.bwsw.tstreamstransactionserver.netty.client.{ClientBuilder, MasterReelectionListener}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.hierarchy.LongZookeeperTreeList
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.BookkeeperOptions
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.StorageOptions
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates.Checkpointed
import com.bwsw.tstreamstransactionserver.rpc.{ProducerTransaction, TransactionInfo}
import com.bwsw.tstreamstransactionserver.util.Utils.{getRandomStream, startZkAndBookieServerWithConfig}
import com.bwsw.tstreamstransactionserver.util.multiNode.CommonCheckpointGroupServerTtlUtils._
import com.bwsw.tstreamstransactionserver.util.multiNode.MultiNodeUtils.getCommonCheckpointGroupCluster
import org.apache.bookkeeper.conf.ServerConfiguration
import org.apache.bookkeeper.meta.{LedgerManager, LedgerManagerFactory}
import org.apache.bookkeeper.proto.BookieServer
import org.apache.bookkeeper.zookeeper.ZooKeeperClient
import org.apache.curator.framework.CuratorFramework
import org.scalatest.{Assertion, Matchers, Outcome, fixture}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Random, Success, Try}

class SlaveHandleWhenSomeLedgerIsExpiredCommonCheckpointGroupServerTest
  extends fixture.FlatSpec
    with Matchers {

  private val ensembleNumber = 3
  private val writeQuorumNumber = 3
  private val ackQuorumNumber = 2

  // because we use CommonCheckpointGroupServer that has two zk trees so creates two times more ledgers
  private val treeFactor = 2
  private val gcWaitTimeMs = 500
  private val entryLogSizeLimit = 1024 * 1024
  private val timeBetweenCreationOfLedges = 1
  private val dataCompactionInterval = Int.MaxValue
  private val ttl = 6
  private val millisBetweenServerStarts = 500
  private val waitResponseSeconds = 10

  private val bookkeeperOptions = BookkeeperOptions(
    ensembleNumber,
    writeQuorumNumber,
    ackQuorumNumber,
    "test".getBytes(),
    dataCompactionInterval)

  private lazy val serverBuilder = new CommonCheckpointGroupServerBuilder()
    .withServerStorageOptions(StorageOptions(dataCompactionInterval = dataCompactionInterval))
  private lazy val clientBuilder = new ClientBuilder()

  private val bookiesNumber =
    ensembleNumber max writeQuorumNumber max ackQuorumNumber


  case class FixtureParam(zkClient: CuratorFramework,
                          bookieServers: Array[(BookieServer, ServerConfiguration)],
                          ledgerManager: LedgerManager)


  override protected def withFixture(test: OneArgTest): Outcome = {
    val (zkServer, zkClient, bookieServers) =
      startZkAndBookieServerWithConfig(bookiesNumber, gcWaitTimeMs, entryLogSizeLimit)

    val zk = ZooKeeperClient.newBuilder.connectString(zkClient.getZookeeperClient.getCurrentConnectionString).build
    //doesn't matter which one's conf because zk is a common part
    val ledgerManagerFactory = LedgerManagerFactory.newLedgerManagerFactory(bookieServers.head._2, zk)
    val ledgerManager = ledgerManagerFactory.newLedgerManager


    val fixtureParam = FixtureParam(zkClient, bookieServers, ledgerManager)

    val testResult = Try(withFixture(test.toNoArgTest(fixtureParam)))

    ledgerManager.close()
    bookieServers.foreach(_._1.shutdown())

    zk.close()
    zkClient.close()
    zkServer.close()

    testResult.get
  }


  "Slave" should "properly handle when expired ledgers is deleted" in { fixture =>
    var createdLedgers = 0
    val stream = getRandomStream
    val partition = 0
    val numberOfEntryLogs = 2
    val dataAmount = 10
    val data = Seq.fill(dataAmount)(Random.nextString(10).getBytes)
    val clusterSize = 2
    val masterId = 0
    val slaveId = 1


    val cluster = getCommonCheckpointGroupCluster(
      fixture.zkClient,
      bookkeeperOptions,
      serverBuilder,
      clientBuilder,
      clusterSize,
      timeBetweenCreationOfLedges)

    val masterPathMonitor = new ZKMasterPathMonitor(
      fixture.zkClient,
      cluster.clientBuilder.getZookeeperOptions.retryDelayMs,
      cluster.clientBuilder.getConnectionOptions.prefix)
    masterPathMonitor.startMonitoringMasterServerPath()

    val masterChangedLatch = new CountDownLatch(1)
    val listener = new MasterReelectionListener {
      override def masterChanged(newMaster: Either[Throwable, Option[SocketHostPortPair]]): Unit = {
        newMaster match {
          case Right(Some(_)) =>
            masterChangedLatch.countDown()
            masterPathMonitor.removeMasterReelectionListener(this)
          case _ =>
        }
      }
    }

    val cgPath = cluster.serverBuilder
      .getCommonPrefixesOptions
      .checkpointGroupPrefixesOptions
      .checkpointGroupZkTreeListPrefix
    val cgTree = new LongZookeeperTreeList(fixture.zkClient, cgPath)

    val commonPath = cluster.serverBuilder.getCommonPrefixesOptions.commonMasterZkTreeListPrefix
    val commonTree = new LongZookeeperTreeList(fixture.zkClient, commonPath)

    val trees = Set(cgTree, commonTree)

    cluster.operate { () =>

      cluster.start(masterId)
      val startMasterTime = System.currentTimeMillis()
      Thread.sleep(millisBetweenServerStarts)
      cluster.start(slaveId)
      cluster.startClient()

      //create the required number of txns to roll over the initial entryLog (0.log)

      val streamId = Await.result(cluster.client.putStream(stream), waitResponseSeconds.seconds)
      fillEntryLog(cluster, numberOfEntryLogs, entryLogSizeLimit)

      val compactionJob = cluster.get(masterId).get.bookKeeperCompactionJob
      compactionJob.compact()

      val elapsedTime = ((System.currentTimeMillis() - startMasterTime) / 1000).toInt

      if (elapsedTime < ttl) {
        createdLedgers = elapsedTime / timeBetweenCreationOfLedges * treeFactor
      } else throw new TimeLimitExceededException("Decrease time of entry logs creation " +
        "because entry logs files will be deleted if the creation time is greater than ttl")


      ledgersExistInZkTree(trees, createdLedgers) shouldBe true
      ledgersExistInBookKeeper(fixture.ledgerManager, createdLedgers) shouldBe true
      entryLogsExistInBookKeeper(fixture.bookieServers.map(_._1), numberOfEntryLogs) shouldBe true

      cluster.stop(slaveId)
      Thread.sleep(toMs(ttl))

      compactionJob.compact()

      val elapsedTimeAfterSecondCompaction = ((System.currentTimeMillis() - startMasterTime) / 1000).toInt

      val createdLedgersAfterSecondCompaction = elapsedTimeAfterSecondCompaction / timeBetweenCreationOfLedges * treeFactor
      ledgersExistInZkTree(trees, createdLedgersAfterSecondCompaction)
      ledgersExistInBookKeeper(fixture.ledgerManager, createdLedgersAfterSecondCompaction)
      entryLogsExistInBookKeeper(fixture.bookieServers.map(_._1), numberOfEntryLogs)

      cluster.start(slaveId)

      val transactionID =
        Await.result(
          cluster.client.putSimpleTransactionAndData(streamId, partition, data),
          waitResponseSeconds.seconds)

      masterPathMonitor.addMasterReelectionListener(listener)
      cluster.stopClient()
      cluster.stop(masterId)

      masterChangedLatch.await(waitResponseSeconds, TimeUnit.SECONDS)


      cluster.startClient()

      def waitUntilClientConnects(timeout: Int): Boolean = {
        if (cluster.client.isConnected) true
        else if (timeout > 0) {
          Thread.sleep(1000)
          waitUntilClientConnects(timeout - 1)
        }
        else false
      }

      waitUntilClientConnects(waitResponseSeconds)

      /** Need to wait until slave read all BookKeeper records */
      def tryGetTransaction(retries: Int): Assertion = {
        Thread.sleep(waitResponseSeconds * 1000)

        Try {
          Await.result(
            cluster.client.getTransaction(streamId, partition, transactionID),
            waitResponseSeconds.seconds)
        } match {
          case Success(transactionInfo) if !transactionInfo.exists && retries > 0 =>
            tryGetTransaction(retries - 1)

          case Success(transactionInfo) =>
            transactionInfo should matchPattern {
              case TransactionInfo(true, Some(ProducerTransaction(`streamId`, `partition`, `transactionID`, Checkpointed, `dataAmount`, _))) =>
            }

          case Failure(_: ReadOperationIsNotAllowedException) if retries > 0 =>
            tryGetTransaction(retries - 1)

          case Failure(exception) => throw exception
        }
      }

      val getTransactionTries = 3 // each trying spent more than waitResponseSeconds time

      tryGetTransaction(getTransactionTries)

      Await.result(
        cluster.client.getTransactionData(streamId, partition, transactionID, 0, dataAmount),
        waitResponseSeconds.seconds) should contain theSameElementsInOrderAs data
    }

    masterPathMonitor.stopMonitoringMasterServerPath()
  }
}
