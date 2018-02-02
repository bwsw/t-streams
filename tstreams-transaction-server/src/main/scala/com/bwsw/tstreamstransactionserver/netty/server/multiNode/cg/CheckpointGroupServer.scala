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

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreamstransactionserver.netty.server._
import com.bwsw.tstreamstransactionserver.netty.server.authService.OpenedTransactions
import com.bwsw.tstreamstransactionserver.netty.server.db.zk.ZookeeperStreamRepository
import com.bwsw.tstreamstransactionserver.netty.server.storage.rocks.MultiNodeRockStorage
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataService
import com.bwsw.tstreamstransactionserver.netty.server.zk.ZookeeperClient
import com.bwsw.tstreamstransactionserver.options.CommonOptions
import com.bwsw.tstreamstransactionserver.options.CommonOptions.TracingOptions
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.{BookkeeperOptions, CheckpointGroupPrefixesOptions}
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions._
import com.bwsw.tstreamstransactionserver.tracing.ServerTracer
import com.bwsw.tstreamstransactionserver.{ExecutionContextGrid, SinglePoolExecutionContextGrid}
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.socket.ServerSocketChannel
import io.netty.channel.{ChannelOption, EventLoopGroup}
import io.netty.handler.logging.{LogLevel, LoggingHandler}
import org.apache.curator.retry.RetryForever

import scala.util.Try

class CheckpointGroupServer(authenticationOpts: AuthenticationOptions,
                            packageTransmissionOpts: TransportOptions,
                            zookeeperOpts: CommonOptions.ZookeeperOptions,
                            bootstrapOpts: BootstrapOptions,
                            checkpointGroupRoleOptions: CheckpointGroupRoleOptions,
                            checkpointGroupPrefixesOptions: CheckpointGroupPrefixesOptions,
                            bookkeeperOptions: BookkeeperOptions,
                            storageOpts: StorageOptions,
                            rocksStorageOpts: RocksStorageOptions,
                            tracingOptions: TracingOptions) {
  private val isShutdown = new AtomicBoolean(false)

  ServerTracer.init(tracingOptions, "TTS-CG")

  private val transactionServerSocketAddress =
    Utils.createTransactionServerExternalSocket(
      bootstrapOpts.bindHost,
      bootstrapOpts.bindPort
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

  private val openedTransactions = OpenedTransactions(authenticationOpts.tokenTtlSec)

  protected lazy val rocksWriter: RocksWriter = new RocksWriter(
    rocksStorage,
    transactionDataService,
    openedTransactions)

  private val bookkeeperToRocksWriter =
    new CheckpointGroupBookkeeperWriter(
      zk.client,
      bookkeeperOptions,
      checkpointGroupPrefixesOptions,
      storageOpts.dataCompactionInterval
    )

  private val checkpointGroupMasterElector =
    zk.masterElector(
      transactionServerSocketAddress,
      checkpointGroupRoleOptions.checkpointGroupMasterPrefix,
      checkpointGroupRoleOptions.checkpointGroupMasterElectionPrefix
    )

  private val checkpointMaster = bookkeeperToRocksWriter
    .createCheckpointMaster(
      checkpointGroupMasterElector,
      zk.idGenerator(checkpointGroupPrefixesOptions.checkpointGroupLastClosedLedger),
      storageOpts.dataCompactionInterval
    )


  private val (
    bossGroup: EventLoopGroup,
    workerGroup: EventLoopGroup,
    channelType: Class[ServerSocketChannel]
    ) = Utils.getBossGroupAndWorkerGroupAndChannel

  private val commitLogContext: SinglePoolExecutionContextGrid =
    ExecutionContextGrid("CommitLogExecutionContextGrid-%d")

  private val requestRouter =
    new CheckpointGroupHandlerRouter(
      checkpointMaster.bookkeeperMaster,
      commitLogContext.getContext,
      packageTransmissionOpts,
      authenticationOpts
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
        .bind(bootstrapOpts.bindHost, bootstrapOpts.bindPort)
        .sync()

      checkpointGroupMasterElector.start()

      checkpointMaster.start()

      val channel = binding.channel().closeFuture()
      function
      channel.sync()
    } finally {
      shutdown()
    }
  }

  def shutdown(): Unit = {
    if (!isShutdown.getAndSet(true)) {
      checkpointMaster.stop()
      checkpointGroupMasterElector.stop()

      Try {
        bossGroup.shutdownGracefully(
          0L,
          0L,
          TimeUnit.NANOSECONDS)
          .cancel(true)
      }
      Try {
        workerGroup.shutdownGracefully(
          0L,
          0L,
          TimeUnit.NANOSECONDS)
          .cancel(true)
      }

      zk.close()
      commitLogContext.stopAccessNewTasks()
      commitLogContext.awaitAllCurrentTasksAreCompleted()
      rocksStorage.getStorageManager.closeDatabases()
      transactionDataService.closeTransactionDataDatabases()
      bookkeeperToRocksWriter.close()
    }
  }
}
