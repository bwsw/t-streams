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


import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.BookKeeperCompactionJob
import com.bwsw.tstreamstransactionserver.netty.server.{Notifier, RocksTestingWriter, RocksWriter}
import com.bwsw.tstreamstransactionserver.options.CommonOptions
import com.bwsw.tstreamstransactionserver.options.CommonOptions.TracingOptions
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.{BookkeeperOptions, CommonPrefixesOptions}
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions._
import com.bwsw.tstreamstransactionserver.rpc.{ConsumerTransaction, ProducerTransaction}

class CommonCheckpointGroupTestingServer(authenticationOpts: AuthenticationOptions,
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
                                         tracingOptions: TracingOptions = TracingOptions())
  extends CommonCheckpointGroupServer(
    authenticationOpts,
    packageTransmissionOpts,
    zookeeperOpts,
    serverOpts,
    commonRoleOptions,
    commonPrefixesOptions,
    checkpointGroupRoleOptions,
    bookkeeperOptions,
    storageOpts,
    rocksStorageOpts,
    subscribersUpdateOptions,
    tracingOptions) {

  def this(builder: CommonCheckpointGroupServerBuilder) = {
    this(
      builder.getAuthenticationOptions,
      builder.getPackageTransmissionOptions,
      builder.getZookeeperOptions,
      builder.getBootstrapOptions,
      builder.getCommonRoleOptions,
      builder.getCommonPrefixesOptions,
      builder.getCheckpointGroupRoleOptions,
      builder.getBookkeeperOptions,
      builder.getStorageOptions,
      builder.getRocksStorageOptions,
      builder.getSubscribersUpdateOptions,
      builder.getTracingOptions)
  }

  override protected lazy val bookkeeperToRocksWriter =
    new CommonCheckpointGroupBookkeeperTestingWriter(
      zk.client,
      bookkeeperOptions,
      commonPrefixesOptions,
      storageOpts.dataCompactionInterval
    )

  override protected lazy val rocksWriter: RocksWriter =
    new RocksTestingWriter(
      rocksStorage,
      transactionDataService,
      producerNotifier,
      consumerNotifier,
      openedTransactions)

  private lazy val producerNotifier = new Notifier[ProducerTransaction]
  private lazy val consumerNotifier = new Notifier[ConsumerTransaction]

  final def notifyProducerTransactionCompleted(onNotificationCompleted: ProducerTransaction => Boolean,
                                               func: => Unit): Long =
    producerNotifier.leaveRequest(onNotificationCompleted, func)

  final def removeNotification(id: Long): Boolean =
    producerNotifier.removeRequest(id)

  final def notifyConsumerTransactionCompleted(onNotificationCompleted: ConsumerTransaction => Boolean,
                                               func: => Unit): Long =
    consumerNotifier.leaveRequest(onNotificationCompleted, func)

  final def removeConsumerNotification(id: Long): Boolean =
    consumerNotifier.removeRequest(id)

  def bookKeeperCompactionJob: BookKeeperCompactionJob =
    bookkeeperToRocksWriter.bookKeeperCompactionJob

  override def shutdown(): Unit = {
    super.shutdown()
    producerNotifier.close()
    producerNotifier.close()
  }
}
