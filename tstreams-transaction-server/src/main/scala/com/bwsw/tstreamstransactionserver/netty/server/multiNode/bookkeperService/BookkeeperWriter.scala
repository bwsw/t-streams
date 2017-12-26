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

package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy.LongZookeeperTreeList
import com.bwsw.tstreamstransactionserver.netty.server.zk.{ZKIDGenerator, ZKMasterElector}
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.BookkeeperOptions
import org.apache.bookkeeper.client.BookKeeper
import org.apache.bookkeeper.conf.ClientConfiguration
import org.apache.bookkeeper.meta.LongHierarchicalLedgerManagerFactory
import org.apache.curator.framework.CuratorFramework

abstract class BookkeeperWriter(zookeeperClient: CuratorFramework,
                                bookkeeperOptions: BookkeeperOptions) {

  protected final val bookKeeper: BookKeeper = {
    val lowLevelZkClient = zookeeperClient.getZookeeperClient
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

  protected final def createMaster(zKMasterElector: ZKMasterElector,
                                   zkLastClosedLedgerHandler: ZKIDGenerator,
                                   timeBetweenCreationOfLedgersMs: Int,
                                   zookeeperTreeListLong: LongZookeeperTreeList): BookkeeperMasterBundle = {
    val zKMasterElectorWrapper =
      new LeaderSelector(zKMasterElector)

    val commonBookkeeperMaster =
      new BookkeeperMaster(
        bookKeeper,
        zkLastClosedLedgerHandler,
        zKMasterElectorWrapper,
        bookkeeperOptions,
        zookeeperTreeListLong,
        timeBetweenCreationOfLedgersMs
      )

    new BookkeeperMasterBundle(
      commonBookkeeperMaster
    )
  }

  def getLastConstructedLedger: Long
}
