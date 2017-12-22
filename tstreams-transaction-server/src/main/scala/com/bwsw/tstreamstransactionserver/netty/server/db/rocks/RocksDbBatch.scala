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
package com.bwsw.tstreamstransactionserver.netty.server.db.rocks

import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDbBatch
import org.rocksdb.{ColumnFamilyHandle, TtlDB, WriteBatch, WriteOptions}

class RocksDbBatch(client: TtlDB,
                   databaseHandlers: collection.immutable.Map[Int, ColumnFamilyHandle])
  extends KeyValueDbBatch() {

  private val batch =
    new WriteBatch()

  def put(index: Int, key: Array[Byte],
          data: Array[Byte]): Boolean = {
    batch.put(
      databaseHandlers(index),
      key,
      data
    )
    true
  }

  def remove(index: Int,
             key: Array[Byte]): Unit =
    batch.remove(databaseHandlers(index), key)

  def write(): Boolean = {
    val writeOptions = new WriteOptions()
    val status = scala.util.Try(
      client.write(writeOptions, batch)
    ) match {
      case scala.util.Success(_) =>
        true
      case scala.util.Failure(throwable) =>
        throwable.printStackTrace()
        false
    }

    writeOptions.close()
    batch.close()
    status
  }
}

