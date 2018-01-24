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


import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService._
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.hierarchy.LongZookeeperTreeList
import com.bwsw.tstreamstransactionserver.netty.server.zk.ZKIDGenerator
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.BookkeeperOptions
import org.apache.bookkeeper.client.BookKeeper
import org.apache.bookkeeper.conf.ClientConfiguration
import org.apache.bookkeeper.meta.LongHierarchicalLedgerManagerFactory
import org.apache.curator.retry.RetryForever
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import util.Utils


class BookkeeperMasterTest
  extends FlatSpec
    with BeforeAndAfterAll
    with Matchers {

  private val ensembleNumber = 4
  private val writeQourumNumber = 3
  private val ackQuorumNumber = 2

  private val bookkeeperOptions =
    BookkeeperOptions(
      ensembleNumber,
      writeQourumNumber,
      ackQuorumNumber,
      "test".getBytes()
    )

  private val masterSelector = new LeaderSelectorInterface {
    override def hasLeadership: Boolean = true

    override def stopParticipateInElection(): Unit = {}
  }

  private val bookiesNumber =
    ensembleNumber max writeQourumNumber max ackQuorumNumber


  private val createNewLedgerEveryTimeMs =
    250

  private def uuid = java.util.UUID.randomUUID.toString


  private lazy val (zkServer, zkClient, bookies) =
    Utils.startZkServerBookieServerZkClient(bookiesNumber)

  private lazy val bookkeeper: BookKeeper = {
    val lowLevelZkClient = zkClient.getZookeeperClient
    val configuration = new ClientConfiguration()
      .setZkServers(
        lowLevelZkClient.getCurrentConnectionString
      )
      .setZkTimeout(lowLevelZkClient.getConnectionTimeoutMs)

    configuration.setLedgerManagerFactoryClass(
      classOf[LongHierarchicalLedgerManagerFactory]
    )

    new BookKeeper(configuration)
  }

  override def beforeAll(): Unit = {
    zkServer
    zkClient
    bookies
  }

  override def afterAll(): Unit = {
    bookies.foreach(_.shutdown())
    zkClient.close()
    zkServer.close()
  }


  "Bookkeeper master" should "return the first created ledger." in {
    val bundle = util.multiNode
      .Util.getTransactionServerBundle(zkClient)

    bundle.operate { _ =>
      val zkTree1 = new LongZookeeperTreeList(zkClient, s"/$uuid")

      val closedLedgerIDGen =
        new ZKIDGenerator(zkClient, new RetryForever(100), s"/$uuid")

      val bookkeeperMaster =
        new BookkeeperMaster(
          bookkeeper,
          closedLedgerIDGen,
          masterSelector,
          bookkeeperOptions,
          zkTree1,
          createNewLedgerEveryTimeMs
        )


      val bookkeeperMasterBundle =
        new BookkeeperMasterBundle(
          bookkeeperMaster
        )

      bookkeeperMasterBundle.start()
      Thread.sleep(createNewLedgerEveryTimeMs)

      bookkeeperMaster.doOperationWithCurrentWriteLedger { currentLedger =>
        currentLedger.isRight shouldBe true
        currentLedger.right.get.getId shouldBe 0
      }

      bookkeeperMasterBundle.stop()
    }
  }

  it should "return new ledger for write operations as previous is closed" in {
    val bundle = util.multiNode
      .Util.getTransactionServerBundle(zkClient)

    bundle.operate { _ =>

      val zkTree1 = new LongZookeeperTreeList(zkClient, s"/$uuid")

      val closedLedgerIDGen =
        new ZKIDGenerator(zkClient, new RetryForever(100), s"/$uuid")

      val bookkeeperMaster =
        new BookkeeperMaster(
          bookkeeper,
          closedLedgerIDGen,
          masterSelector,
          bookkeeperOptions,
          zkTree1,
          createNewLedgerEveryTimeMs
        )

      val bookkeeperMasterBundle =
        new BookkeeperMasterBundle(
          bookkeeperMaster
        )

      bookkeeperMasterBundle.start()
      Thread.sleep(createNewLedgerEveryTimeMs * 3)

      bookkeeperMaster.doOperationWithCurrentWriteLedger { currentLedger =>
        currentLedger.isRight shouldBe true
        currentLedger.right.get.getId should be > 1L
      }

      bookkeeperMasterBundle.stop()
    }
  }
}
