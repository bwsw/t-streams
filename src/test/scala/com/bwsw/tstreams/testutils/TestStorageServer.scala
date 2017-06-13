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

import com.bwsw.tstreamstransactionserver.netty.server.Server
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.ServerBuilder
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{AuthOptions, CommitLogOptions, StorageOptions}

/**
  * Created by Ivan Kudryavtsev on 29.01.17.
  */
object TestStorageServer {

  private val serverBuilder = new ServerBuilder()
    .withZookeeperOptions(new ZookeeperOptions(endpoints = s"127.0.0.1:${TestUtils.ZOOKEEPER_PORT}"))

  private var tempDir: String = TestUtils.getTmpDir()

  def getNewClean(): Server = {
    tempDir = TestUtils.getTmpDir()
    get()
  }

  def get(): Server = {
    val transactionServer = serverBuilder
        .withAuthOptions(new AuthOptions(key = TestUtils.AUTH_KEY))
      .withServerStorageOptions(new StorageOptions(path = tempDir))
      .withCommitLogOptions(new CommitLogOptions(commitLogCloseDelayMs = 100))
      .build()
    val l = new CountDownLatch(1)
    new Thread(() => transactionServer.start(l.countDown())).start()
    l.await()
    transactionServer
  }

  def dispose(transactionServer: Server) = {
    transactionServer.shutdown()
  }

}
