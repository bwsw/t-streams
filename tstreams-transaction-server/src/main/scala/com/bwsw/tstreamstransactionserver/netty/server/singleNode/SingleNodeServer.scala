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
package com.bwsw.tstreamstransactionserver.netty.server.singleNode

import java.util
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, PriorityBlockingQueue, TimeUnit}

import com.bwsw.commitlog.filesystem.{CommitLogCatalogue, CommitLogFile, CommitLogStorage}
import com.bwsw.tstreamstransactionserver.configProperties.ServerExecutionContextGrids
import com.bwsw.tstreamstransactionserver.netty.server._
import com.bwsw.tstreamstransactionserver.netty.server.commitLogService._
import com.bwsw.tstreamstransactionserver.netty.server.db.rocks.RocksDbConnection
import com.bwsw.tstreamstransactionserver.netty.server.db.zk.ZookeeperStreamRepository
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.commitLogService.CommitLogService
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.hanlder.SingleNodeRequestRouter
import com.bwsw.tstreamstransactionserver.netty.server.storage.rocks.MultiAndSingleNodeRockStorage
import com.bwsw.tstreamstransactionserver.netty.server.subscriber.{OpenedTransactionNotifier, SubscriberNotifier, SubscribersObserver}
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataService
import com.bwsw.tstreamstransactionserver.netty.server.zk.ZookeeperClient
import com.bwsw.tstreamstransactionserver.options.CommonOptions
import com.bwsw.tstreamstransactionserver.options.CommonOptions.TracingOptions
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions._
import com.bwsw.tstreamstransactionserver.tracing.ServerTracer
import com.bwsw.tstreamstransactionserver.{ExecutionContextGrid, SinglePoolExecutionContextGrid}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.socket.ServerSocketChannel
import io.netty.channel.{ChannelOption, EventLoopGroup}
import io.netty.handler.logging.{LogLevel, LoggingHandler}
import org.apache.curator.retry.RetryForever


class SingleNodeServer(authenticationOpts: AuthenticationOptions,
                       zookeeperOpts: CommonOptions.ZookeeperOptions,
                       serverOpts: BootstrapOptions,
                       commonRoleOptions: CommonRoleOptions,
                       checkpointGroupRoleOptions: CheckpointGroupRoleOptions,
                       storageOpts: StorageOptions,
                       rocksStorageOpts: RocksStorageOptions,
                       commitLogOptions: CommitLogOptions,
                       packageTransmissionOpts: TransportOptions,
                       subscribersUpdateOptions: SubscriberUpdateOptions,
                       tracingOptions: TracingOptions) {

  //  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
  private val isShutdown = new AtomicBoolean(false)

  ServerTracer.init(tracingOptions, "TTS-S")

  private val transactionServerSocketAddress =
    Util.createTransactionServerExternalSocket(
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

  protected val storage: MultiAndSingleNodeRockStorage =
    new MultiAndSingleNodeRockStorage(
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
    storage,
    transactionDataService
  )

  private val rocksReader = new RocksReader(
    storage,
    transactionDataService
  )

  private val transactionServer = new TransactionServer(
    zkStreamRepository,
    rocksWriter,
    rocksReader
  )

  private val oneNodeCommitLogService =
    new singleNode.commitLogService.CommitLogService(
      storage.getStorageManager
    )

  private val rocksDBCommitLog = new RocksDbConnection(
    rocksStorageOpts,
    s"${storageOpts.path}${java.io.File.separatorChar}${storageOpts.commitLogRocksDirectory}",
    commitLogOptions.expungeDelaySec
  )

  private val commitLogQueue = {
    val queue = new CommitLogQueueBootstrap(
      30,
      new CommitLogCatalogue(storageOpts.path + java.io.File.separatorChar + storageOpts.commitLogRawDirectory),
      oneNodeCommitLogService
    )
    val priorityQueue = queue.fillQueue()
    priorityQueue
  }


  /**
    * this variable is public for testing purposes only
    */
  val commitLogToRocksWriter = new CommitLogToRocksWriter(
    rocksDBCommitLog,
    commitLogQueue,
    rocksWriter,
    oneNodeCommitLogService,
    commitLogOptions.incompleteReadPolicy
  )

  private val fileIDGenerator =
    zk.idGenerator(commitLogOptions.zkFileIdGeneratorPath)


  val scheduledCommitLog = new ScheduledCommitLog(
    commitLogQueue,
    storageOpts,
    commitLogOptions,
    fileIDGenerator
  )

  private val rocksWriterExecutor =
    Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder().setNameFormat("RocksWriter-%d").build()
    )

  private val commitLogCloseExecutor =
    Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder().setNameFormat("CommitLogClose-%d").build()
    )

  private val (
    bossGroup: EventLoopGroup,
    workerGroup: EventLoopGroup,
    channelType: Class[ServerSocketChannel]
    ) = Util.getBossGroupAndWorkerGroupAndChannel

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


  private val requestRouter: SingleNodeRequestRouter =
    new SingleNodeRequestRouter(
      transactionServer,
      oneNodeCommitLogService,
      scheduledCommitLog,
      packageTransmissionOpts,
      authenticationOpts,
      orderedExecutionPool,
      openedTransactionNotifier,
      checkpointGroupRoleOptions,
      executionContext,
      commitLogContext.getContext
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


  private lazy val commitLogCloseTask =
    commitLogCloseExecutor.scheduleWithFixedDelay(
      scheduledCommitLog,
      commitLogOptions.closeDelayMs,
      commitLogOptions.closeDelayMs,
      java.util.concurrent.TimeUnit.MILLISECONDS
    )

  private lazy val commitLogToRocksWriterTask =
    rocksWriterExecutor.scheduleWithFixedDelay(
      commitLogToRocksWriter,
      0L,
      10L,
      java.util.concurrent.TimeUnit.MILLISECONDS
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

      commitLogCloseTask
      commitLogToRocksWriterTask

      commonMasterElector.start()
      checkpointGroupMasterElector.start()

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

      if (rocksWriterExecutor != null) {
        commitLogToRocksWriterTask.cancel(true)
        rocksWriterExecutor.shutdown()
        rocksWriterExecutor.awaitTermination(
          commitLogOptions.closeDelayMs * 5,
          TimeUnit.MILLISECONDS
        )
      }

      if (scheduledCommitLog != null)
        scheduledCommitLog.closeWithoutCreationNewFile()

      if (commitLogCloseExecutor != null) {
        commitLogCloseTask.cancel(true)
        commitLogCloseExecutor.shutdown()
        commitLogCloseExecutor.awaitTermination(
          commitLogOptions.closeDelayMs * 5,
          TimeUnit.MILLISECONDS
        )
      }

      if (commitLogToRocksWriter != null) {
        rocksDBCommitLog.close()
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

      if (storage != null) {
        storage.getStorageManager.closeDatabases()
      }

      if (transactionDataService != null) {
        transactionDataService.closeTransactionDataDatabases()
      }
    }
  }
}

class CommitLogQueueBootstrap(queueSize: Int,
                              commitLogCatalogue: CommitLogCatalogue,
                              commitLogService: CommitLogService) {

  def fillQueue(): PriorityBlockingQueue[CommitLogStorage] = {
    val allFiles = commitLogCatalogue.listAllFilesAndTheirIDs().toMap


    val berkeleyProcessedFileIDMax = commitLogService.getLastProcessedCommitLogFileID
    val (allFilesIDsToProcess, allFilesToDelete: Map[Long, CommitLogFile]) =
      if (berkeleyProcessedFileIDMax > -1)
        (allFiles.filterKeys(_ > berkeleyProcessedFileIDMax), allFiles.filterKeys(_ <= berkeleyProcessedFileIDMax))
      else
        (allFiles, collection.immutable.Map())

    allFilesToDelete.values.foreach(_.delete())

    if (allFilesIDsToProcess.nonEmpty) {
      import scala.collection.JavaConverters.asJavaCollectionConverter
      val filesToProcess: util.Collection[CommitLogFile] = allFilesIDsToProcess
        .map { case (id, _) => new CommitLogFile(allFiles(id).getFile.getPath) }
        .asJavaCollection

      val maxSize = scala.math.max(filesToProcess.size, queueSize)
      val commitLogQueue = new PriorityBlockingQueue[CommitLogStorage](maxSize)

      if (filesToProcess.isEmpty) commitLogQueue
      else if (commitLogQueue.addAll(filesToProcess)) commitLogQueue
      else throw new Exception("Something goes wrong here")
    } else {
      new PriorityBlockingQueue[CommitLogStorage](queueSize)
    }
  }
}