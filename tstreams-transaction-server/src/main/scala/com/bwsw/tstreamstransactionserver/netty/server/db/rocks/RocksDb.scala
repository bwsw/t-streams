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

import com.bwsw.tstreamstransactionserver.netty.server.db.{KeyValueDb, KeyValueDbIterator}
import org.rocksdb._

class RocksDb(client: TtlDB,
              databaseHandler: ColumnFamilyHandle)
  extends KeyValueDb {
  RocksDB.loadLibrary()

  def get(key: Array[Byte]): Array[Byte] = client.get(databaseHandler, key)

  @throws[RocksDBException]
  def put(key: Array[Byte], data: Array[Byte]): Boolean = {
    client.put(databaseHandler, key, data)
    true
  }

  def delete(key: Array[Byte]): Boolean = {
    client.delete(databaseHandler, key)
    true
  }

  def getLastRecord: Option[(Array[Byte], Array[Byte])] = {
    val iter = iterator
    iter.seekToLast()
    val record = if (iter.isValid) {
      val keyValue = (iter.key(), iter.value())
      Some(keyValue)
    }
    else {
      None
    }
    iter.close()
    record
  }

  def iterator: KeyValueDbIterator = new RocksDbIteratorWrapper(client.newIterator(databaseHandler))

  //  def newBatch = new Batch
  //  class Batch() {
  //    private val batch  = new WriteBatch()
  //
  //    def put(key: Array[Byte], data: Array[Byte]): Unit = batch.put(databaseHandler, key, data)
  //    def remove(key: Array[Byte]): Unit = batch.remove(databaseHandler, key)
  //
  //    def write(): Boolean = {
  //      val writeOptions = new WriteOptions()
  //      val status = scala.util.Try(client.write(writeOptions, batch)) match {
  //        case scala.util.Success(_) => true
  //        case scala.util.Failure(throwable) =>
  //          throwable.printStackTrace()
  //          false
  //      }
  //      writeOptions.close()
  //      batch.close()
  //      status
  //    }
  //  }

}
