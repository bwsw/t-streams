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

package com.bwsw.tstreamstransactionserver.netty.server.multiNode.cg

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.hierarchy.LongZookeeperTreeList
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.storage.BookKeeperWrapper
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.{BookKeeperCompactionJob, BookkeeperMasterBundle, BookkeeperWriter}
import com.bwsw.tstreamstransactionserver.netty.server.zk.{ZKIDGenerator, ZKMasterElector}
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.{BookkeeperOptions, CheckpointGroupPrefixesOptions}
import org.apache.curator.framework.CuratorFramework

class CheckpointGroupBookkeeperWriter(zookeeperClient: CuratorFramework,
                                      bookkeeperOptions: BookkeeperOptions,
                                      checkpointGroupPrefixesOptions: CheckpointGroupPrefixesOptions,
                                      compactionInterval: Long)
  extends BookkeeperWriter(
    zookeeperClient,
    bookkeeperOptions
  ) {

  private val checkpointMasterZkTreeList =
    new LongZookeeperTreeList(
      zookeeperClient,
      checkpointGroupPrefixesOptions.checkpointGroupZkTreeListPrefix
    )

  private val maybeCompactionJob =
    if (bookkeeperOptions.expungeDelaySec > 0)
      Some(new BookKeeperCompactionJob(
        Array(checkpointMasterZkTreeList),
        new BookKeeperWrapper(bookKeeper, bookkeeperOptions),
        bookkeeperOptions.expungeDelaySec,
        compactionInterval))
    else None

  maybeCompactionJob.foreach(_.start())

  override def getLastConstructedLedger: Long = {
    checkpointMasterZkTreeList
      .lastEntityId
      .getOrElse(-1L)
  }

  def createCheckpointMaster(zKMasterElector: ZKMasterElector,
                             zkLastClosedLedgerHandler: ZKIDGenerator): BookkeeperMasterBundle = {
    createMaster(
      zKMasterElector,
      zkLastClosedLedgerHandler,
      checkpointGroupPrefixesOptions.timeBetweenCreationOfLedgersMs,
      checkpointMasterZkTreeList
    )
  }

  override def close(): Unit = {
    super.close()
    maybeCompactionJob.foreach(_.close())
  }
}
