
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

package com.bwsw.tstreamstransactionserver.options

import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.options.CommitLogWriteSyncPolicy._
import com.bwsw.tstreamstransactionserver.options.IncompleteCommitLogReadPolicy._
import org.rocksdb._

object SingleNodeServerOptions {

  /** The options are applied on bootstrap of a server.
    *
    * @param bindHost               ipv4 or ipv6 listen address in string representation.
    * @param bindPort               port to a server binds.
    * @param openOperationsPoolSize size of the ordered pool that contains single thread executors to work with some producer transaction operations.
    */
  final case class BootstrapOptions(bindHost: String = "127.0.0.1",
                                    bindPort: Int = 8071,
                                    openOperationsPoolSize: Int = Runtime.getRuntime.availableProcessors())


  /** The options are used to provide to zookeeper
    * a prefix for leader election and
    * a prefix for putting address of server if it's a leader or elected to be a leader(master).
    *
    * @param commonMasterPrefix         the prefix is used for providing current master/leader common group server.
    * @param commonMasterElectionPrefix the prefix is used for leader election among common servers.
    */
  final case class CommonRoleOptions(commonMasterPrefix: String = "/tts/common/master",
                                     commonMasterElectionPrefix: String = "/tts/common/master_election")


  /** The options are used to provide to zookeeper
    * a prefix for leader election and
    * a prefix for putting address of server if it's a leader or elected to be a leader(master).
    *
    * @param checkpointGroupMasterPrefix         the prefix is used for providing current master/leader checkpoint group server.
    * @param checkpointGroupMasterElectionPrefix the prefix is used for leader election among checkpoint group servers.
    */
  final case class CheckpointGroupRoleOptions(checkpointGroupMasterPrefix: String = "/tts/cg/master",
                                              checkpointGroupMasterElectionPrefix: String = "/tts/cg/master_election")

  /** The options are used to provide notification service for subscribers.
    *
    * @param updatePeriodMs        delay in milliseconds between updates of current subscribers online.
    * @param monitoringZkEndpoints The ZooKeeper server(s) connect to.
    */
  final case class SubscriberUpdateOptions(updatePeriodMs: Int = 1000,
                                           monitoringZkEndpoints: Option[String] = None)

  /** The options are used to validate client requests by a server.
    *
    * @param key                       the key to authorize server's clients.
    * @param keyCacheSize              the number of active tokens a server can handle over time.
    * @param keyCacheExpirationTimeSec The lifetime of token after last access before expiration..
    */
  final case class AuthenticationOptions(key: String = "",
                                         keyCacheSize: Int = 10000,
                                         keyCacheExpirationTimeSec: Int = 600)

  /** The options are used to define folders for databases.
    *
    * @param path                     the path where folders of Commit log and rocksdb databases would be placed.
    * @param streamZookeeperDirectory the zooKeeper path for stream entities.
    * @param dataDirectory            the subfolder of [[com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.StorageOptions.path]] where rocksdb databases are placed which contain producer data.
    * @param metadataDirectory        the subfolder of [[com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.StorageOptions.path]] where rocksdb database is placed which contains producer and consumer transactions.
    * @param commitLogRawDirectory    the subfolder of [[com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.StorageOptions.path]] where commit log files are placed.
    * @param commitLogRocksDirectory  The subfolder of [[com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.StorageOptions.path]] where rocksdb database is placed which contains commit log files.
    *
    */
  final case class StorageOptions(path: String = "/tmp",
                                  streamZookeeperDirectory: String = "/tts/streams",
                                  dataDirectory: String = "transaction_data",
                                  metadataDirectory: String = "transaction_metadata",
                                  commitLogRawDirectory: String = "commit_log",
                                  commitLogRocksDirectory: String = "commit_log_rocks" //,
                                  /** streamStorageName: String = "StreamStore", consumerStorageName: String = "ConsumerStore",
                                    * metadataStorageName: String = "TransactionStore", openedTransactionsStorageName: String = "TransactionOpenStore",
                                    * berkeleyReadThreadPool: Int = 2 */)


  /** The options are applied on creation Rocksdb database.
    * For all rocksDB options look: https: //github.com/facebook/rocksdb/blob/master/include/rocksdb/options.h
    * For performance optimization intuition: https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide
    *
    * @param writeThreadPool            the number of threads of pool are used to do write operations to Rocksdb databases.
    *                                   Used for [[com.bwsw.tstreamstransactionserver.netty.server.ServerHandler]]
    * @param readThreadPool             the number of threads of pool are used to do read operations from Rocksdb databases.
    *                                   Used for [[com.bwsw.tstreamstransactionserver.netty.server.ServerHandler]]
    * @param transactionTtlAppendMs     the value to sum with a stream [[com.bwsw.tstreamstransactionserver.rpc.StreamValue.ttl]] attribute, to determine lifetime of all producer transactions data belonging to the stream.
    * @param transactionExpungeDelayMin the lifetime of a producer transaction after persistence to database.(default: 6 months). If negative integer - transactions aren't deleted at all.
    * @param maxBackgroundCompactions   the maximum number of concurrent background compactions. The default is 1, but to fully utilize your CPU and storage you might want to increase this to approximately number of cores in the system.
    * @param compression                compression takes one of values: [NO_COMPRESSION, SNAPPY_COMPRESSION, ZLIB_COMPRESSION, BZLIB2_COMPRESSION, LZ4_COMPRESSION, LZ4HC_COMPRESSION].
    *                                   If it's unimportant use a LZ4_COMPRESSION as default value.
    * @param isFsync                    if true, then every store to stable storage will issue a fsync.
    *                                   If false, then every store to stable storage will issue a fdatasync.
    *                                   This parameter should be set to true while storing data to filesystem like ext3 that can lose files after a reboot.
    */
  final case class RocksStorageOptions(writeThreadPool: Int = 2,
                                       readThreadPool: Int = 2,
                                       transactionTtlAppendMs: Int = 50,
                                       transactionExpungeDelayMin: Int = TimeUnit.DAYS.toMinutes(180).toInt,
                                       maxBackgroundCompactions: Int = 1,
                                       compression: CompressionType = CompressionType.LZ4_COMPRESSION,
                                       isFsync: Boolean = true) {


    def createDBOptions(maxBackgroundCompactions: Int = this.maxBackgroundCompactions,
                        isFsync: Boolean = this.isFsync): DBOptions = {

      new DBOptions()
        .setCreateIfMissing(true)
        .setCreateMissingColumnFamilies(true)
        .setMaxBackgroundCompactions(maxBackgroundCompactions)
        .setUseFsync(isFsync)
        .setRowCache(new LRUCache(100000L))
    }

    def createOptions(maxBackgroundCompactions: Int = this.maxBackgroundCompactions,
                      compression: CompressionType = this.compression,
                      useFsync: Boolean = this.isFsync): Options = {
      new Options()
        .setCreateIfMissing(true)
        .setCompressionType(compression)
        .setMaxBackgroundCompactions(maxBackgroundCompactions)
        .setUseFsync(useFsync)
    }


    def createColumnFamilyOptions(compression: CompressionType = this.compression): ColumnFamilyOptions = {
      new ColumnFamilyOptions()
        .setCompressionType(compression)
    }
  }

  /** The options are applied when client transmit transactions or producer transactions data to server. On setting options also take into consideration [[com.bwsw.tstreamstransactionserver.options.ClientOptions.ConnectionOptions.requestTimeoutMs]].
    *
    * @param maxMetadataPackageSize the size of metadata package that client can transmit or request to/from server, i.e. calling 'scanTransactions' method.
    *                               If client tries to transmit amount of data which is greater than maxMetadataPackageSize or maxDataPackageSize then it gets an exception.
    *                               If server receives a client requests of size which is greater than maxMetadataPackageSize or maxDataPackageSize then it discards them and sends an exception to the client.
    *                               If server during an operation undertands that it is near to overfill constraints it can stop the operation and return a partial dataset.
    * @param maxDataPackageSize     the size of data package that client can transmit or request to/from server, i.e. calling 'getTransactionData' method.
    *                               If client tries to transmit amount of data which is greater than maxMetadataPackageSize or maxDataPackageSize then it gets an exception.
    *                               If server receives a client requests of size which is greater than maxMetadataPackageSize or maxDataPackageSize then it discards them and sends an exception to the client.
    *                               If server during an operation undertands that it is near to overfill constraints it can stop the operation and return a partial dataset.
    */
  final case class TransportOptions(maxMetadataPackageSize: Int = 10000000,
                                    maxDataPackageSize: Int = 10000000)

  /** The options are applied when processing commit log files.
    *
    * @param syncPolicy            policies to work with commitlog.
    *                              If 'every-n-seconds' mode is chosen then data is flushed into file when specified count of seconds from last flush operation passed.
    *                              If 'every-new-file' mode is chosen then data is flushed into file when new file starts.
    *                              If 'every-nth' mode is chosen then data is flushed into file when specified count of write operations passed.
    * @param syncValue             count of write operations or count of seconds between flush operations. It depends on the selected policy.
    * @param incompleteReadPolicy  policies to read from commitlog files.
    *                              If 'resync-majority' mode is chosen then ???(not implemented yet).
    *                              If 'skip-log' mode is chosen commit log files than haven't md5 file are not read.
    *                              If 'try-read' mode is chosen commit log files than haven't md5 file are tried to be read.
    *                              If 'error' mode is chosen commit log files than haven't md5 file throw throwable and stop server working.
    * @param closeDelayMs          the time through which a commit log file is closed.
    * @param expungeDelaySec       the lifetime of commit log files before they are deleted.
    * @param zkFileIdGeneratorPath the coordination path for counter that is used to generate and retrieve commit log file id.
    */
  final case class CommitLogOptions(syncPolicy: CommitLogWriteSyncPolicy = EveryNewFile,
                                    syncValue: Int = 0,
                                    incompleteReadPolicy: IncompleteCommitLogReadPolicy = SkipLog,
                                    closeDelayMs: Int = 200,
                                    expungeDelaySec: Int = 86400,
                                    zkFileIdGeneratorPath: String = "/tts/file_id_gen")

}

