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

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy.{BookkeeperToRocksWriter, LongNodeCache, LongZookeeperTreeList, ZkMultipleTreeListReader}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.storage.BookkeeperWrapper
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.commitLogService.CommitLogService
import com.bwsw.tstreamstransactionserver.netty.server.RocksWriter
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.BookkeeperOptions
import org.apache.bookkeeper.client.BookKeeper

class BookkeeperSlave(bookKeeper: BookKeeper,
                      bookkeeperOptions: BookkeeperOptions,
                      zkTrees: Array[LongZookeeperTreeList],
                      lastClosedLedgersHandlers: Array[LongNodeCache],
                      commitLogService: CommitLogService,
                      rocksWriter: RocksWriter)
  extends Runnable {

  private val bookkeeperToRocksWriter = {
    val bk =
      new BookkeeperWrapper(
        bookKeeper,
        bookkeeperOptions
      )

    val multipleTree =
      new ZkMultipleTreeListReader(
        zkTrees,
        lastClosedLedgersHandlers,
        bk
      )

    new BookkeeperToRocksWriter(
      multipleTree,
      commitLogService,
      rocksWriter
    )
  }

  override def run(): Unit = {
    bookkeeperToRocksWriter.run()
  }
}