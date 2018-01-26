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

package com.bwsw.tstreamstransactionserver.util

import java.io.File
import java.net.ServerSocket
import java.nio.file.{Files, Paths}
import java.util
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.bwsw.tstreamstransactionserver.netty.client.ClientBuilder
import com.bwsw.tstreamstransactionserver.netty.client.api.TTSClient
import com.bwsw.tstreamstransactionserver.netty.server.db.zk.ZookeeperStreamRepository
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.{SingleNodeServerBuilder, TestSingleNodeServer}
import com.bwsw.tstreamstransactionserver.netty.server.storage.rocks.MultiAndSingleNodeRockStorage
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataService
import com.bwsw.tstreamstransactionserver.netty.server.{RocksReader, RocksWriter, TransactionServer, singleNode}
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.{RocksStorageOptions, StorageOptions}
import com.bwsw.tstreamstransactionserver.rpc
import com.bwsw.tstreamstransactionserver.rpc.{ConsumerTransaction, ProducerTransaction, TransactionStates}
import org.apache.bookkeeper.conf.ServerConfiguration
import org.apache.bookkeeper.meta.LongHierarchicalLedgerManagerFactory
import org.apache.bookkeeper.proto.BookieServer
import org.apache.commons.io.FileUtils
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.RetryNTimes
import org.apache.curator.test.TestingServer
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.data.ACL

import scala.collection.mutable.ArrayBuffer
import scala.util.{Random, Try}


object Utils {
  val bookieTmpDirs = ArrayBuffer[String]()
  def uuid: String = java.util.UUID.randomUUID.toString

  private val sessionTimeoutMillis = 1000
  private val connectionTimeoutMillis = 1000


  def startZkServerAndGetIt: (TestingServer, CuratorFramework) = {
    val zkServer = new TestingServer(true)

    val zkClient = CuratorFrameworkFactory.builder
      .sessionTimeoutMs(sessionTimeoutMillis)
      .connectionTimeoutMs(connectionTimeoutMillis)
      .retryPolicy(new RetryNTimes(3, 100))
      .connectString(zkServer.getConnectString)
      .build()

    zkClient.start()
    zkClient.blockUntilConnected(connectionTimeoutMillis, TimeUnit.MILLISECONDS)

    (zkServer, zkClient)
  }


  private val zkLedgersRootPath = "/ledgers"
  private val zkBookiesAvailablePath = s"$zkLedgersRootPath/available"

  def startBookieServer(zkEndpoints: String, bookieNumber: Int, gcWaitTime: Long): (BookieServer, ServerConfiguration) = {

    def createBookieFolder() = {
      val path = Files.createTempDirectory(s"bookie")

      path.toFile.getPath
    }

    def startBookie(): (BookieServer, ServerConfiguration) = {
      val bookieFolder = createBookieFolder()
      bookieTmpDirs += bookieFolder

      val serverConfig = new ServerConfiguration()
        .setBookiePort(Utils.getRandomPort)
        .setZkServers(zkEndpoints)
        .setJournalDirName(bookieFolder)
        .setLedgerDirNames(Array(bookieFolder))
        .setAllowLoopback(true)
        .setJournalFlushWhenQueueEmpty(true)
        .setGcWaitTime(gcWaitTime)

      serverConfig
        .setZkLedgersRootPath(zkLedgersRootPath)

      serverConfig.setLedgerManagerFactoryClass(
        classOf[LongHierarchicalLedgerManagerFactory]
      )

      val server = new BookieServer(serverConfig)
      server.start()
      (server, serverConfig)
    }

    startBookie()
  }

  def startZkServerBookieServerZkClient(serverNumber: Int,
                                        gcWaitTime: Long = 600000):
  (TestingServer, CuratorFramework, Array[BookieServer]) = {
    val (zkServer, zkClient) = startZkServerAndGetIt

    zkClient.create()
      .creatingParentsIfNeeded()
      .withMode(CreateMode.PERSISTENT)
      .withACL(new util.ArrayList[ACL](Ids.OPEN_ACL_UNSAFE))
      .forPath(zkLedgersRootPath)

    zkClient.create()
      .withMode(CreateMode.PERSISTENT)
      .withACL(new util.ArrayList[ACL](Ids.OPEN_ACL_UNSAFE))
      .forPath(zkBookiesAvailablePath)

    val bookies = (0 until serverNumber).map(serverIndex =>
      startBookieServer(
        zkClient.getZookeeperClient.getCurrentConnectionString,
        serverIndex,
        gcWaitTime
      )._1
    ).toArray

    (zkServer, zkClient, bookies)
  }

  def startZkAndBookieServerWithConfig(serverNumber: Int,
                                       gcWaitTime: Long = 600000):
  (TestingServer, CuratorFramework, Array[(BookieServer, ServerConfiguration)]) = {
    val (zkServer, zkClient) = startZkServerAndGetIt

    zkClient.create()
      .creatingParentsIfNeeded()
      .withMode(CreateMode.PERSISTENT)
      .withACL(new util.ArrayList[ACL](Ids.OPEN_ACL_UNSAFE))
      .forPath(zkLedgersRootPath)

    zkClient.create()
      .withMode(CreateMode.PERSISTENT)
      .withACL(new util.ArrayList[ACL](Ids.OPEN_ACL_UNSAFE))
      .forPath(zkBookiesAvailablePath)

    val bookies = (0 until serverNumber).map(serverIndex =>
      startBookieServer(
        zkClient.getZookeeperClient.getCurrentConnectionString,
        serverIndex,
        gcWaitTime
      )
    ).toArray

    (zkServer, zkClient, bookies)
  }

  def getRandomStream = rpc.StreamValue(
    name = Random.nextInt(10000).toString,
    partitions = Random.nextInt(10000),
    description = if (Random.nextBoolean()) Some(Random.nextInt(10000).toString) else None,
    ttl = Long.MaxValue,
    zkPath = None)

  def getRandomProducerTransaction(streamID: Int,
                                   streamObj: rpc.StreamValue,
                                   transactionState: TransactionStates = TransactionStates(
                                     Random.nextInt(TransactionStates.list.length) + 1),
                                   id: Long = System.nanoTime()) =
    ProducerTransaction(
      transactionID = id,
      state = transactionState,
      stream = streamID,
      ttl = Long.MaxValue,
      quantity = 0,
      partition = streamObj.partitions)

  def getRandomConsumerTransaction(streamID: Int, streamObj: rpc.StreamValue) =
    ConsumerTransaction(
      transactionID = scala.util.Random.nextLong(),
      name = Random.nextInt(10000).toString,
      stream = streamID,
      partition = streamObj.partitions)

  def getRandomPort: Int = {
    Try {
      new ServerSocket(0)
    }.map { server =>
      val port = server.getLocalPort
      server.close()
      port
    }.get
  }

  private def testStorageOptions(dbPath: File) = {
    StorageOptions(
      path = dbPath.getPath,
      streamZookeeperDirectory = s"/$uuid"
    )
  }

  private def tempFolder() =
    Files.createTempDirectory("tts").toFile

  def getRocksReaderAndRocksWriter(zkClient: CuratorFramework): RocksReaderAndWriter = {
    val dbPath = tempFolder()
    val storageOptions = testStorageOptions(dbPath)
    val rocksStorageOptions = RocksStorageOptions()

    new RocksReaderAndWriter(zkClient, storageOptions, rocksStorageOptions)
  }

  def getTransactionServerBundle(zkClient: CuratorFramework): TransactionServerBundle = {
    val dbPath = tempFolder()

    val storageOptions = testStorageOptions(dbPath)

    val rocksStorageOptions = RocksStorageOptions()

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

    val oneNodeCommitLogService =
      new singleNode.commitLogService.CommitLogService(
        rocksStorage.getStorageManager
      )

    new TransactionServerBundle(
      transactionServer,
      oneNodeCommitLogService,
      rocksStorage,
      transactionDataService,
      storageOptions,
      rocksStorageOptions
    )
  }

  def startTransactionServer(builder: SingleNodeServerBuilder): ZkSeverAndTransactionServer = {
    val zkTestServer = new TestingServer(true)
    val transactionServer = builder
      .withZookeeperOptions(
        builder.getZookeeperOptions.copy(endpoints = zkTestServer.getConnectString)
      )
      .withBootstrapOptions(
        builder.getBootstrapOptions.copy(bindPort = getRandomPort)
      )
      .build()

    val latch = new CountDownLatch(1)
    new Thread(() => {
      transactionServer.start(latch.countDown())
    }).start()

    latch.await(3000, TimeUnit.SECONDS)

    ZkSeverAndTransactionServer(zkTestServer, transactionServer)
  }

  def startTransactionServerAndClient(zkClient: CuratorFramework,
                                      serverBuilder: SingleNodeServerBuilder,
                                      clientBuilder: ClientBuilder): ZkSeverTxnServerTxnClient = {
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

    val transactionServer = new TestSingleNodeServer(
      updatedBuilder.getAuthenticationOptions,
      updatedBuilder.getZookeeperOptions,
      updatedBuilder.getBootstrapOptions,
      updatedBuilder.getCommonRoleOptions,
      updatedBuilder.getCheckpointGroupRoleOptions,
      updatedBuilder.getStorageOptions,
      updatedBuilder.getRocksStorageOptions,
      updatedBuilder.getCommitLogOptions,
      updatedBuilder.getPackageTransmissionOptions,
      updatedBuilder.getSubscribersUpdateOptions,
      updatedBuilder.getTracingOptions
    )

    val latch = new CountDownLatch(1)
    new Thread(() => {
      transactionServer.start(latch.countDown())
    }).start()

    if (!latch.await(5000, TimeUnit.SECONDS))
      throw new IllegalStateException()

    val connectionOptions = clientBuilder.getConnectionOptions
      .copy(prefix = zKCommonMasterPrefix)
    val zookeeperOptions = clientBuilder.getZookeeperOptions
      .copy(endpoints = zkClient.getZookeeperClient.getCurrentConnectionString)
    val client = clientBuilder
      .withConnectionOptions(connectionOptions)
      .withZookeeperOptions(zookeeperOptions)
      .build()

    new ZkSeverTxnServerTxnClient(transactionServer, client, updatedBuilder)
  }

  def startTransactionServerAndClient(zkClient: CuratorFramework,
                                      serverBuilder: SingleNodeServerBuilder,
                                      clientBuilder: ClientBuilder,
                                      clientsNumber: Int): ZkSeverTxnServerTxnClients = {
    val dbPath = Files.createTempDirectory("tts").toFile

    val streamRepositoryPath = s"/$uuid"

    val zkConnectionString = zkClient.getZookeeperClient.getCurrentConnectionString

    val port = getRandomPort

    val updatedBuilder = serverBuilder
      .withZookeeperOptions(
        serverBuilder.getZookeeperOptions.copy(
          endpoints = zkConnectionString
        )
      )
      .withServerStorageOptions(
        serverBuilder.getStorageOptions.copy(
          path = dbPath.getPath,
          streamZookeeperDirectory = streamRepositoryPath)
      )
      .withBootstrapOptions(
        serverBuilder.getBootstrapOptions.copy(bindPort = port)
      )


    val transactionServer = new TestSingleNodeServer(
      updatedBuilder.getAuthenticationOptions,
      updatedBuilder.getZookeeperOptions,
      updatedBuilder.getBootstrapOptions,
      updatedBuilder.getCommonRoleOptions,
      updatedBuilder.getCheckpointGroupRoleOptions,
      updatedBuilder.getStorageOptions,
      updatedBuilder.getRocksStorageOptions,
      updatedBuilder.getCommitLogOptions,
      updatedBuilder.getPackageTransmissionOptions,
      updatedBuilder.getSubscribersUpdateOptions,
      updatedBuilder.getTracingOptions
    )

    val latch = new CountDownLatch(1)
    new Thread(() => {
      transactionServer.start(latch.countDown())
    }).start()

    if (!latch.await(5000, TimeUnit.SECONDS))
      throw new IllegalStateException()


    val clients: Array[TTSClient] = Array.fill(clientsNumber) {
      new ClientBuilder()
        .withZookeeperOptions(
          ZookeeperOptions(
            endpoints = zkConnectionString
          )
        ).build()
    }

    new ZkSeverTxnServerTxnClients(transactionServer, clients, updatedBuilder)
  }


  def deleteDirectories(parent: String, directories: String*): Unit = {
    directories
      .map(Paths.get(parent, _))
      .map(_.toString)
      .map(new File(_))
      .foreach(FileUtils.deleteDirectory)
  }
}
