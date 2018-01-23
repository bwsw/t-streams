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

package com.bwsw.tstreamstransactionserver.options.loader

import com.bwsw.tstreamstransactionserver.options.CommonOptions.{ServerTypeOptions, TracingOptions, ZookeeperOptions}
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.{BookkeeperOptions, CheckpointGroupPrefixesOptions, CommonPrefixesOptions}
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions._
import com.bwsw.tstreamstransactionserver.options.{CommitLogWriteSyncPolicy, IncompleteCommitLogReadPolicy, SingleNodeServerOptions}
import org.rocksdb.CompressionType

import scala.util.{Failure, Success, Try}


object PropertyFileReader {

  final def loadBootstrapOptions(loader: PropertyFileLoader): BootstrapOptions = {
    implicit val typeTag: Class[BootstrapOptions] = classOf[BootstrapOptions]

    val bindHost =
      loader.castCheck("bootstrap.host", identity)
    val bindPort =
      loader.castCheck("bootstrap.port", prop => prop.toInt)
    val openOperationsPoolSize =
      loader.castCheck("bootstrap.open-ops-pool-size", prop => prop.toInt)

    BootstrapOptions(bindHost, bindPort, openOperationsPoolSize)
  }

  final def loadSubscribersUpdateOptions(loader: PropertyFileLoader): SubscriberUpdateOptions = {
    implicit val typeTag: Class[SubscriberUpdateOptions] = classOf[SubscriberUpdateOptions]

    val updatePeriodMs =
      loader.castCheck("subscribers.update-period-ms", prop => prop.toInt)
    val subscriberMonitoringZkEndpoints =
      scala.util.Try(
        loader.castCheck("subscribers.monitoring-zk-endpoints", identity)
      ).toOption

    SubscriberUpdateOptions(updatePeriodMs, subscriberMonitoringZkEndpoints)
  }

  final def loadServerAuthenticationOptions(loader: PropertyFileLoader): AuthenticationOptions = {
    implicit val typeTag: Class[AuthenticationOptions] = classOf[SingleNodeServerOptions.AuthenticationOptions]

    val key =
      loader.castCheck("authentication.key", identity)
    val keyCacheSize =
      loader.castCheck("authentication.key-cache-size", prop => prop.toInt)
    val keyCacheExpirationTimeSec =
      loader.castCheck("authentication.key-cache-expiration-time-sec", prop => prop.toInt)

    SingleNodeServerOptions.AuthenticationOptions(key, keyCacheSize, keyCacheExpirationTimeSec)
  }

  final def loadServerStorageOptions(loader: PropertyFileLoader): StorageOptions = {
    implicit val typeTag: Class[StorageOptions] = classOf[StorageOptions]

    val path =
      loader.castCheck("storage.file-prefix", identity)

    val streamZookeeperDirectory =
      loader.castCheck("storage.streams.zk-directory", identity)

    val dataDirectory =
      loader.castCheck("storage.data.directory", identity)

    val metadataDirectory =
      loader.castCheck("storage.metadata.directory", identity)

    val commitLogRawDirectory =
      loader.castCheck("storage.commit-log.raw-directory", identity)

    val commitLogRocksDirectory =
      loader.castCheck("storage.commit-log.rocks-directory", identity)

    val compactionInterval = loader.castCheck("storage.data.compaction-interval", _.toLong)

    StorageOptions(
      path,
      streamZookeeperDirectory,
      dataDirectory,
      metadataDirectory,
      commitLogRawDirectory,
      commitLogRocksDirectory,
      compactionInterval
    )
  }


  final def loadServerRocksStorageOptions(loader: PropertyFileLoader): RocksStorageOptions = {
    implicit val typeTag: Class[RocksStorageOptions] = classOf[RocksStorageOptions]

    val writeThreadPool =
      loader.castCheck("rocksdb.write-thread-pool", prop => prop.toInt)

    val readThreadPool =
      loader.castCheck("rocksdb.read-thread-pool", prop => prop.toInt)

    val transactionTtlAppendMs =
      loader.castCheck("rocksdb.transaction-ttl-append-sec", prop => prop.toInt)

    val transactionExpungeDelaySec =
      loader.castCheck("rocksdb.transaction-expunge-delay-sec", prop => prop.toInt)

    val maxBackgroundCompactions =
      loader.castCheck("rocksdb.max-background-compactions", prop => prop.toInt)

    val compressionType =
      loader.castCheck("rocksdb.compression-type", prop => CompressionType.getCompressionType(prop))

    val isFsync =
      loader.castCheck("rocksdb.is-fsync", prop => prop.toBoolean)

    RocksStorageOptions(
      writeThreadPool,
      readThreadPool,
      transactionTtlAppendMs,
      transactionExpungeDelaySec,
      maxBackgroundCompactions,
      compressionType,
      isFsync
    )
  }

  final def loadZookeeperOptions(loader: PropertyFileLoader): ZookeeperOptions = {
    implicit val typeTag: Class[ZookeeperOptions] = classOf[ZookeeperOptions]

    val endpoints =
      loader.castCheck("zk.endpoints", identity)

    val sessionTimeoutMs =
      loader.castCheck("zk.session-timeout-ms", prop => prop.toInt)

    val retryDelayMs =
      loader.castCheck("zk.connection-retry-delay-ms", prop => prop.toInt)

    val connectionTimeoutMs =
      loader.castCheck("zk.connection-timeout-ms", prop => prop.toInt)

    ZookeeperOptions(endpoints, sessionTimeoutMs, retryDelayMs, connectionTimeoutMs)
  }

  final def loadCommonRoleOptions(loader: PropertyFileLoader): CommonRoleOptions = {
    implicit val typeTag: Class[CommonRoleOptions] = classOf[CommonRoleOptions]

    val commonPrefix =
      loader.castCheck("zk.common.prefix", identity)

    val commonElectionPrefix =
      loader.castCheck("zk.common.election-prefix", identity)

    CommonRoleOptions(commonPrefix, commonElectionPrefix)
  }

  final def loadCheckpointGroupRoleOptions(loader: PropertyFileLoader): CheckpointGroupRoleOptions = {
    implicit val typeTag: Class[CheckpointGroupRoleOptions] = classOf[CheckpointGroupRoleOptions]

    val commonPrefix =
      loader.castCheck("zk.checkpointgroup.prefix", identity)

    val commonElectionPrefix =
      loader.castCheck("zk.checkpointgroup.election-prefix", identity)

    CheckpointGroupRoleOptions(commonPrefix, commonElectionPrefix)
  }


  final def loadPackageTransmissionOptions(loader: PropertyFileLoader): TransportOptions = {
    implicit val typeTag: Class[TransportOptions] = classOf[TransportOptions]

    val maxMetadataPackageSize =
      loader.castCheck("network.max-metadata-package-size", prop => prop.toInt)

    val maxDataPackageSize =
      loader.castCheck("network.max-data-package-size", prop => prop.toInt)

    TransportOptions(maxMetadataPackageSize, maxDataPackageSize)
  }

  final def loadCommitLogOptions(loader: PropertyFileLoader): CommitLogOptions = {
    implicit val typeTag: Class[CommitLogOptions] = classOf[CommitLogOptions]

    val writeSyncPolicy =
      loader.castCheck("commit-log.write-sync-policy", prop => CommitLogWriteSyncPolicy.withName(prop))

    val writeSyncValue =
      loader.castCheck("commit-log.write-sync-value", prop => prop.toInt)

    val incompleteReadPolicy =
      loader.castCheck("commit-log.incomplete-read-policy", prop => IncompleteCommitLogReadPolicy.withName(prop))

    val closeDelayMs =
      loader.castCheck("commit-log.close-delay-ms", prop => prop.toInt)

    val expungeDelaySec =
      loader.castCheck("commit-log.rocksdb-expunge-delay-sec", prop => prop.toInt)

    val zkFileIdGeneratorPath =
      loader.castCheck("commit-log.zk-file-id-gen-path", identity)

    CommitLogOptions(
      writeSyncPolicy,
      writeSyncValue,
      incompleteReadPolicy,
      closeDelayMs,
      expungeDelaySec,
      zkFileIdGeneratorPath
    )
  }

  final def loadBookkeeperOptions(loader: PropertyFileLoader): BookkeeperOptions = {
    implicit val typeTag: Class[BookkeeperOptions] = classOf[BookkeeperOptions]

    val ensembleNumber =
      loader.castCheck("ha.ensemble-number", prop => prop.toInt)

    val writeQuorumNumber =
      loader.castCheck("ha.write-quorum-number", prop => prop.toInt)

    val ackQuorumNumber =
      loader.castCheck("ha.ack-quorum-number", prop => prop.toInt)

    val password =
      loader.castCheck("ha.password", prop => prop.getBytes())

    val expungeDelaySec =
      loader.castCheck("ha.expunge-delay-sec", prop => prop.toInt)

    BookkeeperOptions(
      ensembleNumber,
      writeQuorumNumber,
      ackQuorumNumber,
      password,
      expungeDelaySec
    )
  }

  final def loadCheckpointGroupPrefixesOptions(loader: PropertyFileLoader): CheckpointGroupPrefixesOptions = {
    implicit val typeTag: Class[CheckpointGroupPrefixesOptions] = classOf[CheckpointGroupPrefixesOptions]

    val checkpointMasterZkTreeListPrefix =
      loader.castCheck("ha.cg.zk.path", identity)

    val checkpointGroupLastClosedLedger =
      loader.castCheck("ha.cg.last-closed-ledger", identity)

    val timeBetweenCreationOfLedgersMs =
      loader.castCheck("ha.cg.close-delay-ms", prop => prop.toInt)

    CheckpointGroupPrefixesOptions(
      checkpointMasterZkTreeListPrefix,
      checkpointGroupLastClosedLedger,
      timeBetweenCreationOfLedgersMs
    )
  }

  final def loadCommonPrefixesOptions(loader: PropertyFileLoader): CommonPrefixesOptions = {
    implicit val typeTag: Class[CommonPrefixesOptions] = classOf[CommonPrefixesOptions]

    val commonMasterZkTreeListPrefix =
      loader.castCheck("ha.common.zk.path", identity)

    val commonMasterLastClosedLedger =
      loader.castCheck("ha.common.last-closed-ledger", identity)

    val timeBetweenCreationOfLedgersMs =
      loader.castCheck("ha.common.close-delay-ms", prop => prop.toInt)

    CommonPrefixesOptions(
      commonMasterZkTreeListPrefix,
      commonMasterLastClosedLedger,
      timeBetweenCreationOfLedgersMs,
      loadCheckpointGroupPrefixesOptions(loader)
    )
  }


  final def loadTracingOptions(loader: PropertyFileLoader): TracingOptions = {
    implicit val typeTag: Class[TracingOptions] = classOf[TracingOptions]

    val tracingEnabled = Try(loader.castCheck("tracing.enabled", prop => prop.toBoolean)).getOrElse(false)
    if (tracingEnabled) {
      val endpoint = loader.castCheck("tracing.endpoint", identity)

      TracingOptions(tracingEnabled, endpoint)
    } else
      TracingOptions()
  }

  final def loadServerTypeOptions(loader: PropertyFileLoader): ServerTypeOptions = {
    implicit val typeTag: Class[ServerTypeOptions] = classOf[ServerTypeOptions]

    Try(loader.castCheck("server.type", identity)) match {
      case Success(serverType) =>
        if (ServerTypeOptions.types.contains(serverType)) ServerTypeOptions(serverType)
        else throw new IllegalStateException(
          s"Illegal value of option 'server.type=$serverType'. " +
            s"Available values: ${ServerTypeOptions.types.mkString("[", ", ", "]")}")
      case Failure(_) => ServerTypeOptions()
    }
  }
}
