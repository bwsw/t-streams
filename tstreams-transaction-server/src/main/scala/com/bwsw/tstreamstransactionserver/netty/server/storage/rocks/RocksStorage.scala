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

package com.bwsw.tstreamstransactionserver.netty.server.storage.rocks

import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDbBatch
import com.bwsw.tstreamstransactionserver.netty.server.db.rocks.RocksDbDescriptor
import com.bwsw.tstreamstransactionserver.netty.server.storage.Storage
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.{RocksStorageOptions, StorageOptions}
import org.rocksdb.ColumnFamilyOptions

abstract class RocksStorage(storageOpts: StorageOptions,
                            rocksOpts: RocksStorageOptions,
                            readOnly: Boolean = false)
  extends Storage {

  protected val columnFamilyOptions: ColumnFamilyOptions =
    rocksOpts.createColumnFamilyOptions()

  protected val commonDescriptors =
    scala.collection.immutable.Seq(
      RocksDbDescriptor(Storage.lastOpenedTransactionStorageDescriptorInfo, columnFamilyOptions),
      RocksDbDescriptor(Storage.lastCheckpointedTransactionStorageDescriptorInfo, columnFamilyOptions),
      RocksDbDescriptor(Storage.consumerStoreDescriptorInfo, columnFamilyOptions),
      RocksDbDescriptor(Storage.transactionAllStoreDescriptorInfo, columnFamilyOptions,
        TimeUnit.MINUTES.toSeconds(rocksOpts.transactionExpungeDelayMin).toInt
      ),
      RocksDbDescriptor(Storage.transactionOpenStoreDescriptorInfo, columnFamilyOptions)
    )
}
