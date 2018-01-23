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

package com.bwsw.tstreamstransactionserver.util.multiNode

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.commitLogService.CommitLogService
import com.bwsw.tstreamstransactionserver.netty.server.storage.Storage
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataService
import com.bwsw.tstreamstransactionserver.netty.server.{RocksReader, RocksWriter, TransactionServer}
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions

import scala.util.{Failure, Try}

class MultiNodeBundle(val transactionServer: TransactionServer,
                      val rocksWriter: RocksWriter,
                      val rocksReader: RocksReader,
                      val multiNodeCommitLogService: CommitLogService,
                      storage: Storage,
                      transactionDataService: TransactionDataService,
                      val storageOptions: SingleNodeServerOptions.StorageOptions,
                      rocksOptions: SingleNodeServerOptions.RocksStorageOptions) {
  def operate(operation: TransactionServer => Unit): Unit = {
    val tried = Try(operation(transactionServer))
    closeDbsAndDeleteDirectories()
    tried match {
      case Failure(throwable) => throw throwable
      case _ =>
    }
  }

  def closeDbsAndDeleteDirectories(): Unit = {
    storage.getStorageManager.closeDatabases()
    transactionDataService.closeTransactionDataDatabases()
    Util.deleteDirectories(storageOptions)
  }
}

