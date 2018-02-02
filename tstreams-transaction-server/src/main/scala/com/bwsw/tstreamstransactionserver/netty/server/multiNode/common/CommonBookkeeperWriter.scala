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

package com.bwsw.tstreamstransactionserver.netty.server.multiNode.common

import com.bwsw.tstreamstransactionserver.netty.server.RocksWriter
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.hierarchy.{LongNodeCache, LongZookeeperTreeList}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService._
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.storage.BookKeeperWrapper
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.commitLogService.CommitLogService
import com.bwsw.tstreamstransactionserver.netty.server.zk.{ZKIDGenerator, ZKMasterElector}
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.{BookkeeperOptions, CommonPrefixesOptions}
import org.apache.curator.framework.CuratorFramework

class CommonBookkeeperWriter(zookeeperClient: CuratorFramework,
                             bookkeeperOptions: BookkeeperOptions,
                             commonPrefixesOptions: CommonPrefixesOptions)
  extends BookkeeperWriter(
    zookeeperClient,
    bookkeeperOptions) {

  private val commonMasterZkTreeList =
    new LongZookeeperTreeList(
      zookeeperClient,
      commonPrefixesOptions.commonMasterZkTreeListPrefix
    )

  private val checkpointMasterZkTreeList =
    new LongZookeeperTreeList(
      zookeeperClient,
      commonPrefixesOptions.checkpointGroupPrefixesOptions.checkpointGroupZkTreeListPrefix
    )

  private val commonMasterLastClosedLedger =
    new LongNodeCache(
      zookeeperClient,
      commonPrefixesOptions
        .commonMasterLastClosedLedger
    )

  private val checkpointMasterLastClosedLedger =
    new LongNodeCache(
      zookeeperClient,
      commonPrefixesOptions
        .checkpointGroupPrefixesOptions
        .checkpointGroupLastClosedLedger
    )

  private val zkTreesList =
    Array(commonMasterZkTreeList, checkpointMasterZkTreeList)

  private val lastClosedLedgerHandlers =
    Array(commonMasterLastClosedLedger, checkpointMasterLastClosedLedger)
  lastClosedLedgerHandlers.foreach(_.startMonitor())

  override def getLastConstructedLedger: Long = {
    val ledgerIds =
      for {
        zkTree <- zkTreesList
        lastConstructedLedgerId <- zkTree.lastEntityId
      } yield lastConstructedLedgerId

    if (ledgerIds.isEmpty) {
      -1L
    } else {
      ledgerIds.max
    }
  }

  def createCommonMaster(zKMasterElector: ZKMasterElector,
                         zkLastClosedLedgerHandler: ZKIDGenerator,
                         compactionInterval: Long): BookkeeperMasterBundle = {
    createMaster(
      zKMasterElector,
      zkLastClosedLedgerHandler,
      commonPrefixesOptions.timeBetweenCreationOfLedgersMs,
      commonMasterZkTreeList,
      compactionInterval
    )
  }

  def createSlave(commitLogService: CommitLogService,
                  rocksWriter: RocksWriter): BookkeeperSlaveBundle = {
    val bookkeeperSlave =
      new BookkeeperSlave(
        bookKeeper,
        bookkeeperOptions,
        zkTreesList,
        lastClosedLedgerHandlers,
        commitLogService,
        rocksWriter,
      )

    val timeBetweenCreationOfLedgersMs = math.max(
      commonPrefixesOptions
        .checkpointGroupPrefixesOptions
        .timeBetweenCreationOfLedgersMs,
      commonPrefixesOptions
        .timeBetweenCreationOfLedgersMs
    )

    new BookkeeperSlaveBundle(
      bookkeeperSlave,
      lastClosedLedgerHandlers,
      timeBetweenCreationOfLedgersMs
    )
  }
}
