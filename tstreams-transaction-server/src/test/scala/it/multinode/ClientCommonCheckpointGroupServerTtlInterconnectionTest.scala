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
package it.multinode

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

import java.util.concurrent.{CountDownLatch, TimeUnit}

import util.multiNode.CommonCheckpointGroupServerTtlUtils._
import com.bwsw.tstreamstransactionserver.netty.client.ClientBuilder
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.CommonCheckpointGroupServerBuilder
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.hierarchy.LongZookeeperTreeList
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.BookkeeperOptions
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.StorageOptions
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates.Checkpointed
import com.bwsw.tstreamstransactionserver.rpc.{TransactionInfo, _}
import org.apache.bookkeeper.meta.{LedgerManager, LedgerManagerFactory}
import org.apache.bookkeeper.zookeeper.ZooKeeperClient
import org.apache.curator.framework.CuratorFramework
import org.scalatest.{Matchers, Outcome, fixture}
import util.Utils.{getRandomProducerTransaction, getRandomStream}
import util.multiNode.ZkServerTxnMultiNodeServerTxnClient

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Random, Success, Try}

class ClientCommonCheckpointGroupServerTtlInterconnectionTest extends fixture.FlatSpec with Matchers {

  private val ensembleNumber = 3
  private val writeQuorumNumber = 3
  private val ackQuorumNumber = 2

  /**
    * because we use CommonCheckpointGroupServer that has two zk trees so creates two times more ledgers
    */
  private val treeFactor = 2
  private val waitMs = 200
  private val maxIdleTimeBetweenRecords = 1
  private val dataCompactionInterval = maxIdleTimeBetweenRecords * 4
  private val ttl = dataCompactionInterval * 2
  private val secondsWait = 5

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
                          ledgerManager: LedgerManager)

  def withFixture(test: OneArgTest): Outcome = {
    val (zkServer, zkClient, bookieServers) =
      util.Utils.startZkAndBookieServerWithConfig(bookiesNumber, waitMs)

    val zk = ZooKeeperClient.newBuilder.connectString(zkClient.getZookeeperClient.getCurrentConnectionString).build
    //doesn't matter which one's conf because zk is a common part
    val ledgerManagerFactory = LedgerManagerFactory.newLedgerManagerFactory(bookieServers.head._2, zk)
    val ledgerManager = ledgerManagerFactory.newLedgerManager

    val fixtureParam = FixtureParam(zkClient, ledgerManager)

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

  "Client" should "receive transactions even though expired ledgers are deleted according to settings " +
    "if a server works in a stable way" in { fixture =>
    val bundle: ZkServerTxnMultiNodeServerTxnClient = util.multiNode.Util.getCommonCheckpointGroupServerBundle(
      fixture.zkClient, bookkeeperOptions, serverBuilder, clientBuilder, toMs(maxIdleTimeBetweenRecords)
    )
    val cgPath = bundle.serverBuilder.getCommonPrefixesOptions.checkpointGroupPrefixesOptions.checkpointGroupZkTreeListPrefix
    val cgTree = new LongZookeeperTreeList(fixture.zkClient, cgPath)

    val commonPath = bundle.serverBuilder.getCommonPrefixesOptions.commonMasterZkTreeListPrefix
    val commonTree = new LongZookeeperTreeList(fixture.zkClient, commonPath)

    val trees = Set(cgTree, commonTree)

    bundle.operate(server => {
      val client = bundle.client
      val stream = getRandomStream
      val partitions = stream.partitions
      val streamId = Await.result(client.putStream(stream), secondsWait.seconds)
      val dataAmount = 10
      val data = Array.fill(dataAmount)(Random.nextString(10).getBytes)

      val openedTransaction = getRandomProducerTransaction(streamId, stream, TransactionStates.Opened)
      val latch1 = new CountDownLatch(1)
      server.notifyProducerTransactionCompleted(
        txn => txn.transactionID == openedTransaction.transactionID,
        latch1.countDown()
      )

      Await.result(client.putProducerStateWithData(openedTransaction, data, 0), secondsWait.seconds)

      latch1.await(maxIdleTimeBetweenRecords * 2, TimeUnit.SECONDS) shouldBe true

      //verify that the first transaction has been opened and has data
      Await.result(
        client.getTransaction(streamId, partitions, openedTransaction.transactionID
        ), secondsWait.seconds) shouldBe TransactionInfo(exists = true, Some(openedTransaction))
      Await.result(
        client.getTransactionData(
          streamId, partitions, openedTransaction.transactionID, 0, dataAmount
        ), secondsWait.seconds) should contain theSameElementsInOrderAs data

      //verify that a compaction job works properly
      Thread.sleep(toMs(dataCompactionInterval))
      val createdLedgers = (dataCompactionInterval / maxIdleTimeBetweenRecords) * treeFactor
      ledgersExistInBookKeeper(fixture.ledgerManager, createdLedgers) shouldBe true
      ledgersExistInZkTree(trees, createdLedgers) shouldBe true

      Thread.sleep(toMs(ttl) + waitMs)

      ledgersExistInBookKeeper(fixture.ledgerManager, (ttl / maxIdleTimeBetweenRecords) * treeFactor + createdLedgers) shouldBe false
      ledgersExistInZkTree(trees, (ttl / maxIdleTimeBetweenRecords) * treeFactor + createdLedgers) shouldBe false

      //checkpoint the first transaction
      val checkpointedTransaction = openedTransaction.copy(state = TransactionStates.Checkpointed)

      //verify that the first transaction is checkpointed and has data after deleting of expired ledgers
      val latch2 = new CountDownLatch(1)
      server.notifyProducerTransactionCompleted(
        txn => txn.transactionID == checkpointedTransaction.transactionID,
        latch2.countDown()
      )
      Await.result(client.putProducerState(checkpointedTransaction), secondsWait.seconds) shouldBe true
      latch2.await(maxIdleTimeBetweenRecords * 2, TimeUnit.SECONDS) shouldBe true
      Await.result(
        client.getTransaction(streamId, partitions, openedTransaction.transactionID
        ), secondsWait.seconds) shouldBe TransactionInfo(exists = true, Some(checkpointedTransaction))
      Await.result(
        client.getTransactionData(
          streamId, partitions, openedTransaction.transactionID, 0, dataAmount
        ), secondsWait.seconds) should contain theSameElementsInOrderAs data

      //create another transaction and checkpoint it
      val simpleTransactionId = Await.result(client.putSimpleTransactionAndData(streamId, partitions, data), secondsWait.seconds)

      //verify that the second transaction is checkpointed and has data
      Thread.sleep(toMs(maxIdleTimeBetweenRecords) * 2)
      Await.result(
        client.getTransaction(streamId, partitions, simpleTransactionId),
        secondsWait.second) should matchPattern {
        case TransactionInfo(true, Some(ProducerTransaction(`streamId`, `partitions`, `simpleTransactionId`, Checkpointed, `dataAmount`, _))) =>
      }
      Await.result(
        client.getTransactionData(streamId, partitions, simpleTransactionId, 0, dataAmount),
        secondsWait.seconds) should contain theSameElementsInOrderAs data
    })
  }
}
