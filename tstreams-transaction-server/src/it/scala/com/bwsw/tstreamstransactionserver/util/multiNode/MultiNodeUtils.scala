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

package com.bwsw.tstreamstransactionserver.util.multiNode

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreamstransactionserver.netty.client.ClientBuilder
import com.bwsw.tstreamstransactionserver.netty.server.db.zk.ZookeeperStreamRepository
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.{CommonCheckpointGroupServerBuilder, CommonCheckpointGroupTestingServer}
import com.bwsw.tstreamstransactionserver.netty.server.storage.rocks.MultiAndSingleNodeRockStorage
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataService
import com.bwsw.tstreamstransactionserver.netty.server.{RocksReader, RocksWriter, TransactionServer, multiNode}
import com.bwsw.tstreamstransactionserver.options.ClientOptions.ConnectionOptions
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.{BookkeeperOptions, CheckpointGroupPrefixesOptions}
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.{RocksStorageOptions, StorageOptions}
import com.bwsw.tstreamstransactionserver.util.Utils
import com.bwsw.tstreamstransactionserver.util.Utils.{getRandomPort, uuid}
import org.apache.commons.io.FileUtils
import org.apache.curator.framework.CuratorFramework

object MultiNodeUtils {
  private def testStorageOptions(dbPath: File) = {
    StorageOptions().copy(
      path = dbPath.getPath,
      streamZookeeperDirectory = s"/$uuid"
    )
  }

  private def tempFolder() = {
    Files.createTempDirectory("tts").toFile
  }

  def getTransactionServerBundle(zkClient: CuratorFramework): MultiNodeBundle = {
    val dbPath = tempFolder()

    val storageOptions =
      testStorageOptions(dbPath)

    val rocksStorageOptions =
      RocksStorageOptions()

    val rocksStorage =
      new MultiAndSingleNodeRockStorage(
        storageOptions,
        rocksStorageOptions
      )

    val zkStreamRepository =
      new ZookeeperStreamRepository(
        zkClient,
        storageOptions.streamZookeeperDirectory
      )

    val transactionDataService =
      new TransactionDataService(
        storageOptions,
        rocksStorageOptions,
        zkStreamRepository
      )

    val rocksWriter =
      new RocksWriter(
        rocksStorage,
        transactionDataService
      )

    val rocksReader =
      new RocksReader(
        rocksStorage,
        transactionDataService
      )

    val transactionServer =
      new TransactionServer(
        zkStreamRepository,
        rocksWriter,
        rocksReader
      )

    val multiNodeCommitLogService =
      new multiNode.commitLogService.CommitLogService(
        rocksStorage.getStorageManager
      )

    new MultiNodeBundle(
      transactionServer,
      rocksWriter,
      rocksReader,
      multiNodeCommitLogService,
      rocksStorage,
      transactionDataService,
      storageOptions,
      rocksStorageOptions
    )
  }

  def getCommonCheckpointGroupServerBundle(zkClient: CuratorFramework,
                                           bookkeeperOptions: BookkeeperOptions,
                                           serverBuilder: CommonCheckpointGroupServerBuilder,
                                           clientBuilder: ClientBuilder,
                                           timeBetweenCreationOfLedgesMs: Int = 200): ZkServerTxnMultiNodeServerTxnClient = {
    val dbPath = Files.createTempDirectory("tts").toFile
    val zKCommonMasterPrefix = s"/$uuid"

    val updatedBuilder = serverBuilder
      .withCommonRoleOptions(
        serverBuilder.getCommonRoleOptions.copy(
          commonMasterPrefix = zKCommonMasterPrefix,
          commonMasterElectionPrefix = s"/$uuid")
      )
      .withZookeeperOptions(
        serverBuilder.getZookeeperOptions.copy(
          endpoints = zkClient.getZookeeperClient.getCurrentConnectionString
        )
      )
      .withServerStorageOptions(
        serverBuilder.getStorageOptions.copy(
          path = dbPath.getPath,
          streamZookeeperDirectory = s"/$uuid")
      )
      .withBootstrapOptions(
        serverBuilder.getBootstrapOptions.copy(bindPort = getRandomPort)
      )
      .withCommonPrefixesOptions(
        serverBuilder.getCommonPrefixesOptions.copy(
          s"/tree/common/$uuid",
          s"/tree/common/$uuid",
          timeBetweenCreationOfLedgesMs,
          CheckpointGroupPrefixesOptions(
            s"/tree/cg/$uuid",
            s"/tree/cg/$uuid",
            timeBetweenCreationOfLedgesMs
          )
        )
      )
      .withBookkeeperOptions(bookkeeperOptions)


    val transactionServer =
      new CommonCheckpointGroupTestingServer(
        updatedBuilder.getAuthenticationOptions,
        updatedBuilder.getPackageTransmissionOptions,
        updatedBuilder.getZookeeperOptions,
        updatedBuilder.getBootstrapOptions,
        updatedBuilder.getCommonRoleOptions,
        updatedBuilder.getCommonPrefixesOptions,
        updatedBuilder.getCheckpointGroupRoleOptions,
        updatedBuilder.getBookkeeperOptions,
        updatedBuilder.getStorageOptions,
        updatedBuilder.getRocksStorageOptions,
        updatedBuilder.getSubscribersUpdateOptions,
        updatedBuilder.getTracingOptions
      )

    val latch = new CountDownLatch(1)
    new Thread(() => {
      transactionServer.start(latch.countDown())
    }).start()

    if (!latch.await(5000, TimeUnit.SECONDS))
      throw new IllegalStateException()

    val client = new ClientBuilder()
      .withConnectionOptions(ConnectionOptions(prefix = zKCommonMasterPrefix))
      .withZookeeperOptions(
        ZookeeperOptions(
          endpoints = zkClient.getZookeeperClient.getCurrentConnectionString
        )
      )
      .build()

    new ZkServerTxnMultiNodeServerTxnClient(transactionServer, client, updatedBuilder)
  }

  def deleteDirectories(storageOptions: StorageOptions): Unit = {
    Utils.bookieTmpDirs.foreach(dir => FileUtils.deleteDirectory(new File(dir)))
    Utils.bookieTmpDirs.clear()
    FileUtils.deleteDirectory(new File(Paths.get(storageOptions.path, storageOptions.metadataDirectory).toString))
    FileUtils.deleteDirectory(new File(Paths.get(storageOptions.path, storageOptions.dataDirectory).toString))
    FileUtils.deleteDirectory(new File(Paths.get(storageOptions.path, storageOptions.commitLogRawDirectory).toString))
    FileUtils.deleteDirectory(new File(Paths.get(storageOptions.path, storageOptions.commitLogRocksDirectory).toString))
    new File(storageOptions.path).delete()
  }
}
