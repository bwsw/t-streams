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

package com.bwsw.tstreams.testutils

import java.util.concurrent.CountDownLatch

import com.bwsw.tstreamstransactionserver.netty.server.singleNode.TestSingleNodeServer
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions._

/**
  * Created by Ivan Kudryavtsev on 29.01.17.
  */
object TestStorageServer {

  private var tempDir: String = TestUtils.getTmpDir()

  def getNewClean(builder: Builder = Builder()): TestSingleNodeServer = {
    tempDir = TestUtils.getTmpDir()

    get(builder)
  }

  def get(builder: Builder = Builder()): TestSingleNodeServer = {
    val transactionServer = builder.build()

    val l = new CountDownLatch(1)
    new Thread(() => transactionServer.start(l.countDown())).start()
    l.await()
    transactionServer
  }

  def dispose(transactionServer: TestSingleNodeServer): Unit =
    transactionServer.shutdown()


  case class Builder(authenticationOptions: AuthenticationOptions = Builder.Defaults.authenticationOptions,
                     zookeeperOptions: ZookeeperOptions = Builder.Defaults.zookeeperOptions,
                     bootstrapOptions: BootstrapOptions = Builder.Defaults.bootstrapOptions,
                     commonRoleOptions: CommonRoleOptions = Builder.Defaults.commonRoleOptions,
                     checkpointGroupRoleOptions: CheckpointGroupRoleOptions = Builder.Defaults.checkpointGroupRoleOptions,
                     storageOptions: StorageOptions = Builder.Defaults.storageOptions,
                     rocksStorageOptions: RocksStorageOptions = Builder.Defaults.rocksStorageOptions,
                     commitLogOptions: CommitLogOptions = Builder.Defaults.commitLogOptions,
                     transportOptions: TransportOptions = Builder.Defaults.transportOptions,
                     subscribersUpdateOptions: SubscriberUpdateOptions = Builder.Defaults.subscribersUpdateOptions) {

    def build(): TestSingleNodeServer = {
      new TestSingleNodeServer(
        authenticationOpts = authenticationOptions,
        zookeeperOpts = zookeeperOptions,
        serverOpts = bootstrapOptions,
        commonRoleOptions = commonRoleOptions,
        checkpointGroupRoleOptions = checkpointGroupRoleOptions,
        storageOpts = storageOptions,
        rocksStorageOpts = rocksStorageOptions,
        commitLogOptions = commitLogOptions,
        packageTransmissionOpts = transportOptions,
        subscribersUpdateOptions = subscribersUpdateOptions)
    }
  }

  object Builder {

    object Defaults {
      val authenticationOptions: AuthenticationOptions = AuthenticationOptions(key = TestUtils.AUTH_KEY)
      val zookeeperOptions: ZookeeperOptions = ZookeeperOptions(endpoints = s"127.0.0.1:${TestUtils.ZOOKEEPER_PORT}")
      val bootstrapOptions: BootstrapOptions = BootstrapOptions()
      val commonRoleOptions: CommonRoleOptions = CommonRoleOptions(commonMasterPrefix = TestUtils.MASTER_PREFIX)
      val checkpointGroupRoleOptions: CheckpointGroupRoleOptions = CheckpointGroupRoleOptions()
      val storageOptions: StorageOptions = StorageOptions(path = tempDir)
      val rocksStorageOptions: RocksStorageOptions = RocksStorageOptions()
      val commitLogOptions: CommitLogOptions = CommitLogOptions(closeDelayMs = 100)
      val transportOptions: TransportOptions = TransportOptions()
      val subscribersUpdateOptions: SubscriberUpdateOptions = SubscriberUpdateOptions()
    }

  }

}
