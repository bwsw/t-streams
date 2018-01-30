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


import com.bwsw.tstreamstransactionserver.netty.server.{Notifier, RocksWriter, TestRocksWriter}
import com.bwsw.tstreamstransactionserver.options.CommonOptions
import com.bwsw.tstreamstransactionserver.options.CommonOptions.TracingOptions
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions._
import com.bwsw.tstreamstransactionserver.rpc.{ConsumerTransaction, ProducerTransaction}


class SingleNodeTestingServer(authenticationOpts: AuthenticationOptions,
                              zookeeperOpts: CommonOptions.ZookeeperOptions,
                              serverOpts: BootstrapOptions,
                              commonRoleOptions: CommonRoleOptions,
                              checkpointGroupRoleOptions: CheckpointGroupRoleOptions,
                              storageOpts: StorageOptions,
                              rocksStorageOpts: RocksStorageOptions,
                              commitLogOptions: CommitLogOptions,
                              packageTransmissionOpts: TransportOptions,
                              subscribersUpdateOptions: SubscriberUpdateOptions,
                              tracingOptions: TracingOptions = TracingOptions())
  extends SingleNodeServer(
    authenticationOpts,
    zookeeperOpts,
    serverOpts,
    commonRoleOptions,
    checkpointGroupRoleOptions,
    storageOpts,
    rocksStorageOpts,
    commitLogOptions,
    packageTransmissionOpts,
    subscribersUpdateOptions,
    tracingOptions) {

  override protected lazy val rocksWriter: RocksWriter =
    new TestRocksWriter(
      storage,
      transactionDataService,
      producerNotifier,
      consumerNotifier
    )
  private lazy val producerNotifier =
    new Notifier[ProducerTransaction]
  private lazy val consumerNotifier =
    new Notifier[ConsumerTransaction]

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

  override def shutdown(): Unit = {
    super.shutdown()
    producerNotifier.close()
    producerNotifier.close()
  }
}
