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

package util


import java.io.File

import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDbBatch
import com.bwsw.tstreamstransactionserver.netty.server.db.zk.ZookeeperStreamRepository
import com.bwsw.tstreamstransactionserver.netty.server.storage.rocks.MultiAndSingleNodeRockStorage
import com.bwsw.tstreamstransactionserver.netty.server.streamService.StreamService
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataService
import com.bwsw.tstreamstransactionserver.netty.server.{RocksReader, RocksWriter}
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.{RocksStorageOptions, StorageOptions}
import org.apache.commons.io.FileUtils
import org.apache.curator.framework.CuratorFramework
import util.multiNode.Util



class RocksReaderAndWriter(zkClient: CuratorFramework,
                           val storageOptions: StorageOptions,
                           rocksStorageOpts: RocksStorageOptions)
{

  private val rocksStorage =
    new MultiAndSingleNodeRockStorage(
      storageOptions,
      rocksStorageOpts
    )

  private val streamRepository =
    new ZookeeperStreamRepository(zkClient, s"${storageOptions.streamZookeeperDirectory}")

  private val transactionDataService =
    new TransactionDataService(
      storageOptions,
      rocksStorageOpts,
      streamRepository
    )

  val rocksWriter = new RocksWriter(
    rocksStorage,
    transactionDataService
  )

  val rocksReader = new RocksReader(
    rocksStorage,
    transactionDataService
  )

  val streamService = new StreamService(
    streamRepository
  )

  def newBatch: KeyValueDbBatch =
    rocksWriter.getNewBatch

  def closeDBAndDeleteFolder(): Unit = {
    rocksStorage.getStorageManager.closeDatabases()
    transactionDataService.closeTransactionDataDatabases()
    Util.deleteDirectories(storageOptions)
  }
}
