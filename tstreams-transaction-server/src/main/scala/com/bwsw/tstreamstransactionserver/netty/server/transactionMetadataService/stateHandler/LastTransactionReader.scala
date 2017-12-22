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
package com.bwsw.tstreamstransactionserver.netty.server.transactionMetadataService.stateHandler


import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDbManager
import com.bwsw.tstreamstransactionserver.netty.server.storage.Storage


class LastTransactionReader(rocksMetaServiceDB: KeyValueDbManager) {
  private final val lastTransactionDatabase =
    rocksMetaServiceDB.getDatabase(Storage.LAST_OPENED_TRANSACTION_STORAGE)

  private final val lastCheckpointedTransactionDatabase =
    rocksMetaServiceDB.getDatabase(Storage.LAST_CHECKPOINTED_TRANSACTION_STORAGE)

  final def getLastTransaction(streamID: Int,
                               partition: Int): Option[LastTransaction] = {

    val key = KeyStreamPartition(streamID, partition)
    val binaryKey = key.toByteArray
    val lastOpenedTransaction =
      Option(lastTransactionDatabase.get(binaryKey))
        .map(data => TransactionId.fromByteArray(data))

    lastOpenedTransaction.map { openedTxn =>
      val lastCheckpointedTransaction =
        Option(lastCheckpointedTransactionDatabase.get(binaryKey))
          .map(data => TransactionId.fromByteArray(data))

      LastTransaction(openedTxn, lastCheckpointedTransaction)
    }
  }
}
