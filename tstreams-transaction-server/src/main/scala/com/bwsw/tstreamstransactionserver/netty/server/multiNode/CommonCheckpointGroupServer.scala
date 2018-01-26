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

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContextGrids
import com.bwsw.tstreamstransactionserver.netty.server._
import com.bwsw.tstreamstransactionserver.netty.server.db.zk.ZookeeperStreamRepository
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.commitLogService.CommitLogService
import com.bwsw.tstreamstransactionserver.netty.server.storage.rocks.MultiNodeRockStorage
import com.bwsw.tstreamstransactionserver.netty.server.subscriber.{OpenedTransactionNotifier, SubscriberNotifier, SubscribersObserver}
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataService
import com.bwsw.tstreamstransactionserver.netty.server.zk.ZookeeperClient
import com.bwsw.tstreamstransactionserver.options.CommonOptions
import com.bwsw.tstreamstransactionserver.options.CommonOptions.TracingOptions
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.{BookkeeperOptions, CommonPrefixesOptions}
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions._
import com.bwsw.tstreamstransactionserver.tracing.ServerTracer
import com.bwsw.tstreamstransactionserver.{ExecutionContextGrid, SinglePoolExecutionContextGrid}
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.socket.ServerSocketChannel
import io.netty.channel.{ChannelOption, EventLoopGroup}
import io.netty.handler.logging.{LogLevel, LoggingHandler}
import org.apache.curator.retry.RetryForever

class CommonCheckpointGroupServer(authenticationOpts: AuthenticationOptions,
                                  packageTransmissionOpts: TransportOptions,
                                  zookeeperOpts: CommonOptions.ZookeeperOptions,
                                  serverOpts: BootstrapOptions,
                                  commonRoleOptions: CommonRoleOptions,
                                  commonPrefixesOptions: CommonPrefixesOptions,
                                  checkpointGroupRoleOptions: CheckpointGroupRoleOptions,
                                  bookkeeperOptions: BookkeeperOptions,
                                  storageOpts: StorageOptions,
                                  rocksStorageOpts: RocksStorageOptions,
                                  subscribersUpdateOptions: SubscriberUpdateOptions,
                                  tracingOptions: TracingOptions) {
  private val isShutdown = new AtomicBoolean(false)

  ServerTracer.init(tracingOptions, "TTS-M")

  private val transactionServerSocketAddress =
    Utils.createTransactionServerExternalSocket(
      serverOpts.bindHost,
      serverOpts.bindPort
    )

  private val zk =
    new ZookeeperClient(
      zookeeperOpts.endpoints,
      zookeeperOpts.sessionTimeoutMs,
      zookeeperOpts.connectionTimeoutMs,
      new RetryForever(zookeeperOpts.retryDelayMs)
    )


  protected val rocksStorage: MultiNodeRockStorage =
    new MultiNodeRockStorage(
      storageOpts,
      rocksStorageOpts
    )

  private val zkStreamRepository: ZookeeperStreamRepository =
    zk.streamRepository(s"${storageOpts.streamZookeeperDirectory}")

  protected val transactionDataService: TransactionDataService =
    new TransactionDataService(
      storageOpts,
      rocksStorageOpts,
      zkStreamRepository
    )

  protected lazy val rocksWriter: RocksWriter = new RocksWriter(
    rocksStorage,
    transactionDataService
  )

  private val rocksReader = new RocksReader(
    rocksStorage,
    transactionDataService
  )

  private val multiNodeCommitLogService =
    new CommitLogService(
      rocksStorage.getStorageManager
    )

  private val transactionServer = new TransactionServer(
    zkStreamRepository,
    rocksWriter,
    rocksReader
  )

  private val bookkeeperToRocksWriter =
    new CommonCheckpointGroupBookkeeperWriter(
      zk.client,
      bookkeeperOptions,
      commonPrefixesOptions,
      storageOpts.dataCompactionInterval
    )

  private val commonMasterElector =
    zk.masterElector(
      transactionServerSocketAddress,
      commonRoleOptions.commonMasterPrefix,
      commonRoleOptions.commonMasterElectionPrefix
    )

  private val checkpointGroupMasterElector =
    zk.masterElector(
      transactionServerSocketAddress,
      checkpointGroupRoleOptions.checkpointGroupMasterPrefix,
      checkpointGroupRoleOptions.checkpointGroupMasterElectionPrefix
    )


  private val commonMaster = bookkeeperToRocksWriter
    .createCommonMaster(
      commonMasterElector,
      zk.idGenerator(
        commonPrefixesOptions
          .commonMasterLastClosedLedger
      )
    )

  private val checkpointMaster = bookkeeperToRocksWriter
    .createCheckpointMaster(
      checkpointGroupMasterElector,
      zk.idGenerator(
        commonPrefixesOptions
          .checkpointGroupPrefixesOptions
          .checkpointGroupLastClosedLedger
      )
    )

  private val slave = bookkeeperToRocksWriter
    .createSlave(
      multiNodeCommitLogService,
      rocksWriter
    )


  private val (
    bossGroup: EventLoopGroup,
    workerGroup: EventLoopGroup,
    channelType: Class[ServerSocketChannel]
    ) = Utils.getBossGroupAndWorkerGroupAndChannel

  private val orderedExecutionPool =
    new OrderedExecutionContextPool(serverOpts.openOperationsPoolSize)


  private val curatorSubscriberClient =
    subscribersUpdateOptions.monitoringZkEndpoints.map {
      monitoringZkEndpoints =>
        if (monitoringZkEndpoints == zookeeperOpts.endpoints) {
          zk
        }
        else {
          new ZookeeperClient(
            monitoringZkEndpoints,
            zookeeperOpts.sessionTimeoutMs,
            zookeeperOpts.connectionTimeoutMs,
            new RetryForever(zookeeperOpts.retryDelayMs)
          )
        }
    }.getOrElse(zk)

  private val openedTransactionNotifier =
    new OpenedTransactionNotifier(
      new SubscribersObserver(
        curatorSubscriberClient.client,
        zkStreamRepository,
        subscribersUpdateOptions.updatePeriodMs
      ),
      new SubscriberNotifier
    )

  private val executionContext =
    new ServerExecutionContextGrids(
      rocksStorageOpts.readThreadPool,
      rocksStorageOpts.writeThreadPool
    )

  private val commitLogContext: SinglePoolExecutionContextGrid =
    ExecutionContextGrid("CommitLogExecutionContextGrid-%d")


  private val requestRouter =
    new CommonCheckpointGroupHandlerRouter(
      transactionServer,
      bookkeeperToRocksWriter,
      commonMaster.bookkeeperMaster,
      checkpointMaster.bookkeeperMaster,
      Seq(
        commonMasterElector,
        checkpointGroupMasterElector
      ),
      multiNodeCommitLogService,
      packageTransmissionOpts,
      authenticationOpts,
      orderedExecutionPool,
      openedTransactionNotifier,
      checkpointGroupRoleOptions,
      executionContext,
      commitLogContext.getContext
    )

  def start(function: => Unit = ()): Unit = {
    try {
      val b = new ServerBootstrap()
      b.group(bossGroup, workerGroup)
        .channel(channelType)
        .handler(new LoggingHandler(LogLevel.DEBUG))
        .childHandler(
          new ServerInitializer(requestRouter)
        )
        .option[java.lang.Integer](ChannelOption.SO_BACKLOG, 128)
        .childOption[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, false)

      val binding = b
        .bind(serverOpts.bindHost, serverOpts.bindPort)
        .sync()

      commonMasterElector.start()
      checkpointGroupMasterElector.start()

      commonMaster.start()
      checkpointMaster.start()
      slave.start()

      val channel = binding.channel().closeFuture()
      function
      channel.sync()
    } finally {
      shutdown()
    }
  }

  def shutdown(): Unit = {
    val isNotShutdown =
      isShutdown.compareAndSet(false, true)

    if (isNotShutdown) {
      if (commonMaster != null) {
        commonMaster.stop()
      }

      if (checkpointMaster != null) {
        checkpointMaster.stop()
      }

      if (commonMasterElector != null)
        commonMasterElector.stop()

      if (checkpointGroupMasterElector != null)
        checkpointGroupMasterElector.stop()

      if (bossGroup != null) {
        scala.util.Try {
          bossGroup.shutdownGracefully(
            0L,
            0L,
            TimeUnit.NANOSECONDS
          ).cancel(true)
        }
      }
      if (workerGroup != null) {
        scala.util.Try {
          workerGroup.shutdownGracefully(
            0L,
            0L,
            TimeUnit.NANOSECONDS
          ).cancel(true)
        }
      }

      if (zk != null && curatorSubscriberClient != null) {
        if (zk == curatorSubscriberClient) {
          zk.close()
        }
        else {
          zk.close()
          curatorSubscriberClient.close()
        }
      }

      if (slave != null) {
        slave.stop()
      }

      if (orderedExecutionPool != null) {
        orderedExecutionPool.close()
      }

      if (commitLogContext != null) {
        commitLogContext.stopAccessNewTasks()
        commitLogContext.awaitAllCurrentTasksAreCompleted()
      }

      if (executionContext != null) {
        executionContext
          .stopAccessNewTasksAndAwaitAllCurrentTasksAreCompleted()
      }

      if (transactionDataService != null) {
        transactionDataService.closeTransactionDataDatabases()
      }

      if (rocksStorage != null) {
        rocksStorage.getStorageManager.closeDatabases()
      }

      if (bookkeeperToRocksWriter != null) {
        bookkeeperToRocksWriter.close()
      }
    }
  }

}
