package com.bwsw.tstreamstransactionserver.options.loader

import com.bwsw.tstreamstransactionserver.options.CommonOptions.{TracingOptions, ZookeeperOptions}
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.{BookkeeperOptions, CheckpointGroupPrefixesOptions, CommonPrefixesOptions}
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions._
import com.bwsw.tstreamstransactionserver.options.{CommitLogWriteSyncPolicy, IncompleteCommitLogReadPolicy, SingleNodeServerOptions}
import org.rocksdb.CompressionType

import scala.util.Try


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
      loader.castCheck("storage-model.file-prefix", identity)

    val streamZookeeperDirectory =
      loader.castCheck("storage-model.streams.zk-directory", identity)

    val dataDirectory =
      loader.castCheck("storage-model.data.directory", identity)

    val metadataDirectory =
      loader.castCheck("storage-model.metadata.directory", identity)

    val commitLogRawDirectory =
      loader.castCheck("storage-model.commit-log.raw-directory", identity)

    val commitLogRocksDirectory =
      loader.castCheck("storage-model.commit-log.rocks-directory", identity)

    StorageOptions(
      path,
      streamZookeeperDirectory,
      dataDirectory,
      metadataDirectory,
      commitLogRawDirectory,
      commitLogRocksDirectory
    )
  }


  final def loadServerRocksStorageOptions(loader: PropertyFileLoader): RocksStorageOptions = {
    implicit val typeTag: Class[RocksStorageOptions] = classOf[RocksStorageOptions]

    val writeThreadPool =
      loader.castCheck("rocksdb.write-thread-pool", prop => prop.toInt)

    val readThreadPool =
      loader.castCheck("rocksdb.read-thread-pool", prop => prop.toInt)

    val transactionTtlAppendMs =
      loader.castCheck("rocksdb.transaction-ttl-append-ms", prop => prop.toInt)

    val transactionExpungeDelayMin =
      loader.castCheck("rocksdb.transaction-expunge-delay-min", prop => prop.toInt)

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
      transactionExpungeDelayMin,
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
      loader.castCheck("replicable.ensemble-number", prop => prop.toInt)

    val writeQuorumNumber =
      loader.castCheck("replicable.write-quorum-number", prop => prop.toInt)

    val ackQuorumNumber =
      loader.castCheck("replicable.ack-quorum-number", prop => prop.toInt)

    val password =
      loader.castCheck("replicable.password", prop => prop.getBytes())

    BookkeeperOptions(
      ensembleNumber,
      writeQuorumNumber,
      ackQuorumNumber,
      password
    )
  }

  final def loadCheckpointGroupPrefixesOptions(loader: PropertyFileLoader): CheckpointGroupPrefixesOptions = {
    implicit val typeTag: Class[CheckpointGroupPrefixesOptions] = classOf[CheckpointGroupPrefixesOptions]

    val checkpointMasterZkTreeListPrefix =
      loader.castCheck("replicable.cg.zk.path", identity)

    val checkpointGroupLastClosedLedger =
      loader.castCheck("replicable.cg.last-closed-ledger", identity)

    val timeBetweenCreationOfLedgersMs =
      loader.castCheck("replicable.cg.close-delay-ms", prop => prop.toInt)

    CheckpointGroupPrefixesOptions(
      checkpointMasterZkTreeListPrefix,
      checkpointGroupLastClosedLedger,
      timeBetweenCreationOfLedgersMs
    )
  }

  final def loadCommonPrefixesOptions(loader: PropertyFileLoader): CommonPrefixesOptions = {
    implicit val typeTag: Class[CommonPrefixesOptions] = classOf[CommonPrefixesOptions]

    val commonMasterZkTreeListPrefix =
      loader.castCheck("replicable.common.zk.path", identity)

    val commonMasterLastClosedLedger =
      loader.castCheck("replicable.common.last-closed-ledger", identity)

    val timeBetweenCreationOfLedgersMs =
      loader.castCheck("replicable.common.close-delay-ms", prop => prop.toInt)

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
}
