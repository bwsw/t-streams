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

import com.bwsw.tstreamstransactionserver.options.CommonOptions
import com.bwsw.tstreamstransactionserver.options.CommonOptions.{TracingOptions, ZookeeperOptions}
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions._
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions._

class CheckpointGroupServerBuilder private(authenticationOpts: AuthenticationOptions,
                                           packageTransmissionOpts: TransportOptions,
                                           zookeeperOpts: CommonOptions.ZookeeperOptions,
                                           bootstrapOpts: BootstrapOptions,
                                           checkpointGroupRoleOpts: CheckpointGroupRoleOptions,
                                           checkpointGroupPrefixesOpts: CheckpointGroupPrefixesOptions,
                                           bookkeeperOpts: BookkeeperOptions,
                                           storageOpts: StorageOptions,
                                           rocksStorageOpts: RocksStorageOptions,
                                           tracingOpts: TracingOptions) {

  private val authenticationOptions = authenticationOpts
  private val packageTransmissionOptions = packageTransmissionOpts
  private val zookeeperOptions = zookeeperOpts
  private val bootstrapOptions = bootstrapOpts
  private val checkpointGroupRoleOptions = checkpointGroupRoleOpts
  private val checkpointGroupPrefixesOptions = checkpointGroupPrefixesOpts
  private val bookkeeperOptions = bookkeeperOpts
  private val storageOptions = storageOpts
  private val rocksStorageOptions = rocksStorageOpts
  private val tracingOptions = tracingOpts

  def this() = this(
    AuthenticationOptions(),
    TransportOptions(),
    CommonOptions.ZookeeperOptions(),
    BootstrapOptions(),
    CheckpointGroupRoleOptions(),
    CheckpointGroupPrefixesOptions(),
    BookkeeperOptions(),
    StorageOptions(),
    RocksStorageOptions(),
    TracingOptions()
  )

  def withAuthenticationOptions(authenticationOptions: AuthenticationOptions) =
    new CheckpointGroupServerBuilder(authenticationOptions, packageTransmissionOptions, zookeeperOptions, bootstrapOptions, checkpointGroupRoleOptions, checkpointGroupPrefixesOptions, bookkeeperOptions, storageOptions, rocksStorageOptions, tracingOptions)

  def withPackageTransmissionOptions(packageTransmissionOptions: TransportOptions) =
    new CheckpointGroupServerBuilder(authenticationOptions, packageTransmissionOptions, zookeeperOptions, bootstrapOptions, checkpointGroupRoleOptions, checkpointGroupPrefixesOptions, bookkeeperOptions, storageOptions, rocksStorageOptions, tracingOptions)

  def withZookeeperOptions(zookeeperOptions: ZookeeperOptions) =
    new CheckpointGroupServerBuilder(authenticationOptions, packageTransmissionOptions, zookeeperOptions, bootstrapOptions, checkpointGroupRoleOptions, checkpointGroupPrefixesOptions, bookkeeperOptions, storageOptions, rocksStorageOptions, tracingOptions)

  def withBootstrapOptions(bootstrapOptions: BootstrapOptions) =
    new CheckpointGroupServerBuilder(authenticationOptions, packageTransmissionOptions, zookeeperOptions, bootstrapOptions, checkpointGroupRoleOptions, checkpointGroupPrefixesOptions, bookkeeperOptions, storageOptions, rocksStorageOptions, tracingOptions)

  def withCheckpointGroupRoleOptions(checkpointGroupRoleOptions: CheckpointGroupRoleOptions) =
    new CheckpointGroupServerBuilder(authenticationOptions, packageTransmissionOptions, zookeeperOptions, bootstrapOptions, checkpointGroupRoleOptions, checkpointGroupPrefixesOptions, bookkeeperOptions, storageOptions, rocksStorageOptions, tracingOptions)

  def withCheckpointGroupPrefixesOptions(checkpointGroupPrefixesOptions: CheckpointGroupPrefixesOptions) =
    new CheckpointGroupServerBuilder(authenticationOptions, packageTransmissionOptions, zookeeperOptions, bootstrapOptions, checkpointGroupRoleOptions, checkpointGroupPrefixesOptions, bookkeeperOptions, storageOptions, rocksStorageOptions, tracingOptions)

  def withBookkeeperOptions(bookkeeperOptions: BookkeeperOptions) =
    new CheckpointGroupServerBuilder(authenticationOptions, packageTransmissionOptions, zookeeperOptions, bootstrapOptions, checkpointGroupRoleOptions, checkpointGroupPrefixesOptions, bookkeeperOptions, storageOptions, rocksStorageOptions, tracingOptions)

  def withServerStorageOptions(storageOptions: StorageOptions) =
    new CheckpointGroupServerBuilder(authenticationOptions, packageTransmissionOptions, zookeeperOptions, bootstrapOptions, checkpointGroupRoleOptions, checkpointGroupPrefixesOptions, bookkeeperOptions, storageOptions, rocksStorageOptions, tracingOptions)

  def withServerRocksStorageOptions(rocksStorageOptions: RocksStorageOptions) =
    new CheckpointGroupServerBuilder(authenticationOptions, packageTransmissionOptions, zookeeperOptions, bootstrapOptions, checkpointGroupRoleOptions, checkpointGroupPrefixesOptions, bookkeeperOptions, storageOptions, rocksStorageOptions, tracingOptions)

  def withTracingOptions(tracingOptions: TracingOptions) =
    new CheckpointGroupServerBuilder(authenticationOptions, packageTransmissionOptions, zookeeperOptions, bootstrapOptions, checkpointGroupRoleOptions, checkpointGroupPrefixesOptions, bookkeeperOptions, storageOptions, rocksStorageOptions, tracingOptions)


  def build() = new CheckpointGroupServer(
    authenticationOptions,
    packageTransmissionOptions,
    zookeeperOptions,
    bootstrapOptions,
    checkpointGroupRoleOptions,
    checkpointGroupPrefixesOptions,
    bookkeeperOptions,
    storageOptions,
    rocksStorageOptions,
    tracingOptions
  )

  def getAuthenticationOptions =
    authenticationOptions.copy()

  def getPackageTransmissionOptions =
    packageTransmissionOptions.copy()

  def getZookeeperOptions =
    zookeeperOptions.copy()

  def getBootstrapOptions =
    bootstrapOptions.copy()

  def getCheckpointGroupRoleOptions =
    checkpointGroupRoleOptions.copy()

  def getCheckpointGroupPrefixesOptions =
    checkpointGroupPrefixesOptions.copy()

  def getBookkeeperOptions =
    bookkeeperOptions.copy()

  def getStorageOptions =
    storageOptions.copy()

  def getRocksStorageOptions =
    rocksStorageOptions.copy()

  def getTracingOptions = tracingOptions
}
