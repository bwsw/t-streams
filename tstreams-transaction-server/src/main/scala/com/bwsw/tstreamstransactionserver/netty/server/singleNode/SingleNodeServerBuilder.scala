
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

import com.bwsw.tstreamstransactionserver.options.CommonOptions
import com.bwsw.tstreamstransactionserver.options.CommonOptions.{TracingOptions, ZookeeperOptions}
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions._

class SingleNodeServerBuilder private(authenticationOpts: AuthenticationOptions,
                                      zookeeperOpts: CommonOptions.ZookeeperOptions,
                                      bootstrapOpts: BootstrapOptions,
                                      commonRoleOpts: CommonRoleOptions,
                                      checkpointGroupRoleOpts: CheckpointGroupRoleOptions,
                                      storageOpts: StorageOptions,
                                      rocksStorageOpts: RocksStorageOptions,
                                      commitLogOpts: CommitLogOptions,
                                      packageTransmissionOpts: TransportOptions,
                                      subscriberUpdateOpts: SubscriberUpdateOptions,
                                      tracingOpts: TracingOptions) {

  private val authenticationOptions = authenticationOpts
  private val zookeeperOptions = zookeeperOpts
  private val bootstrapOptions = bootstrapOpts
  private val commonRoleOptions = commonRoleOpts
  private val checkpointGroupRoleOptions = checkpointGroupRoleOpts
  private val storageOptions = storageOpts
  private val rocksStorageOptions = rocksStorageOpts
  private val commitLogOptions = commitLogOpts
  private val packageTransmissionOptions = packageTransmissionOpts
  private val subscribersUpdateOptions = subscriberUpdateOpts
  private val tracingOptions = tracingOpts

  def this() = this(
    AuthenticationOptions(),
    CommonOptions.ZookeeperOptions(),
    BootstrapOptions(),
    CommonRoleOptions(),
    CheckpointGroupRoleOptions(),
    StorageOptions(),
    RocksStorageOptions(),
    CommitLogOptions(),
    TransportOptions(),
    SubscriberUpdateOptions(),
    TracingOptions()
  )

  def withAuthenticationOptions(authenticationOptions: AuthenticationOptions) =
    new SingleNodeServerBuilder(authenticationOptions, zookeeperOptions, bootstrapOptions, commonRoleOptions, checkpointGroupRoleOptions, storageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions, subscribersUpdateOptions, tracingOptions)

  def withZookeeperOptions(zookeeperOptions: ZookeeperOptions) =
    new SingleNodeServerBuilder(authenticationOptions, zookeeperOptions, bootstrapOptions, commonRoleOptions, checkpointGroupRoleOptions, storageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions, subscribersUpdateOptions, tracingOptions)

  def withBootstrapOptions(bootstrapOptions: BootstrapOptions) =
    new SingleNodeServerBuilder(authenticationOptions, zookeeperOptions, bootstrapOptions, commonRoleOptions, checkpointGroupRoleOptions, storageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions, subscribersUpdateOptions, tracingOptions)

  def withCommonRoleOptions(commonRoleOptions: CommonRoleOptions) =
    new SingleNodeServerBuilder(authenticationOptions, zookeeperOptions, bootstrapOptions, commonRoleOptions, checkpointGroupRoleOptions, storageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions, subscribersUpdateOptions, tracingOptions)

  def withCheckpointGroupRoleOptions(checkpointGroupRoleOptions: CheckpointGroupRoleOptions) =
    new SingleNodeServerBuilder(authenticationOptions, zookeeperOptions, bootstrapOptions, commonRoleOptions, checkpointGroupRoleOptions, storageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions, subscribersUpdateOptions, tracingOptions)

  def withServerStorageOptions(serverStorageOptions: StorageOptions) =
    new SingleNodeServerBuilder(authenticationOptions, zookeeperOptions, bootstrapOptions, commonRoleOptions, checkpointGroupRoleOptions, serverStorageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions, subscribersUpdateOptions, tracingOptions)

  def withServerRocksStorageOptions(serverStorageRocksOptions: RocksStorageOptions) =
    new SingleNodeServerBuilder(authenticationOptions, zookeeperOptions, bootstrapOptions, commonRoleOptions, checkpointGroupRoleOptions, storageOptions, serverStorageRocksOptions, commitLogOptions, packageTransmissionOptions, subscribersUpdateOptions, tracingOptions)

  def withCommitLogOptions(commitLogOptions: CommitLogOptions) =
    new SingleNodeServerBuilder(authenticationOptions, zookeeperOptions, bootstrapOptions, commonRoleOptions, checkpointGroupRoleOptions, storageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions, subscribersUpdateOptions, tracingOptions)

  def withPackageTransmissionOptions(packageTransmissionOptions: TransportOptions) =
    new SingleNodeServerBuilder(authenticationOptions, zookeeperOptions, bootstrapOptions, commonRoleOptions, checkpointGroupRoleOptions, storageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions, subscribersUpdateOptions, tracingOptions)

  def withSubscribersUpdateOptions(subscriberUpdateOptions: SubscriberUpdateOptions): SingleNodeServerBuilder =
    new SingleNodeServerBuilder(authenticationOptions, zookeeperOptions, bootstrapOptions, commonRoleOptions, checkpointGroupRoleOptions, storageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions, subscriberUpdateOptions, tracingOptions)

  def withTracingOptions(tracingOptions: TracingOptions) =
    new SingleNodeServerBuilder(authenticationOptions, zookeeperOptions, bootstrapOptions, commonRoleOptions, checkpointGroupRoleOptions, storageOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions, subscribersUpdateOptions, tracingOptions)

  def build() = new SingleNodeServer(
    authenticationOptions,
    zookeeperOptions,
    bootstrapOptions,
    commonRoleOptions,
    checkpointGroupRoleOptions,
    storageOptions,
    rocksStorageOptions,
    commitLogOptions,
    packageTransmissionOptions,
    subscribersUpdateOptions,
    tracingOptions
  )

  def getZookeeperOptions =
    zookeeperOptions.copy()

  def getAuthenticationOptions =
    authenticationOptions.copy()

  def getBootstrapOptions =
    bootstrapOptions.copy()

  def getCommonRoleOptions =
    commonRoleOptions.copy()

  def getCheckpointGroupRoleOptions =
    checkpointGroupRoleOptions.copy()

  def getStorageOptions =
    storageOptions.copy()

  def getRocksStorageOptions =
    rocksStorageOptions.copy()

  def getPackageTransmissionOptions =
    packageTransmissionOptions.copy()

  def getCommitLogOptions =
    commitLogOptions.copy()

  def getSubscribersUpdateOptions =
    subscribersUpdateOptions.copy()

  def getTracingOptions = tracingOptions
}