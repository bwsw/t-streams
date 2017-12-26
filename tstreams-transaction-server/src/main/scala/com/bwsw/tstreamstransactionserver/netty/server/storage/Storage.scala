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

package com.bwsw.tstreamstransactionserver.netty.server.storage

import com.bwsw.tstreamstransactionserver.netty.server.db.{DbMeta, KeyValueDbBatch, KeyValueDbManager}

object Storage {
  private[storage] final val lastOpenedTransactionStorageDescriptorInfo =
    DbMeta("LastOpenedTransactionStorage")
  private[storage] final val lastCheckpointedTransactionStorageDescriptorInfo =
    DbMeta("LastCheckpointedTransactionStorage")
  private[storage] final val consumerStoreDescriptorInfo =
    DbMeta("ConsumerStore")
  private[storage] final val transactionAllStoreDescriptorInfo =
    DbMeta("TransactionAllStore")
  private[storage] final val transactionOpenStoreDescriptorInfo =
    DbMeta("TransactionOpenStore")
  private[storage] final val commitLogStoreDescriptorInfo =
    DbMeta("CommitLogStore")
  private[storage] final val bookkeeperLogStoreDescriptorInfo =
    DbMeta("BookkeeperLogStore")

  val LAST_OPENED_TRANSACTION_STORAGE: Int =
    lastOpenedTransactionStorageDescriptorInfo.id

  val LAST_CHECKPOINTED_TRANSACTION_STORAGE: Int =
    lastCheckpointedTransactionStorageDescriptorInfo.id

  val CONSUMER_STORE: Int =
    consumerStoreDescriptorInfo.id

  val TRANSACTION_ALL_STORE: Int =
    transactionAllStoreDescriptorInfo.id

  val TRANSACTION_OPEN_STORE: Int =
    transactionOpenStoreDescriptorInfo.id

  val COMMIT_LOG_STORE: Int =
    commitLogStoreDescriptorInfo.id

  val BOOKKEEPER_LOG_STORE: Int =
    bookkeeperLogStoreDescriptorInfo.id
}

trait Storage {
  def getStorageManager: KeyValueDbManager

  final def newBatch: KeyValueDbBatch =
    getStorageManager.newBatch
}
