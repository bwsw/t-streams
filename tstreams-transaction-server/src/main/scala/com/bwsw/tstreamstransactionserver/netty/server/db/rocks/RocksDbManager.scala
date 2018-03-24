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

import java.io.File
import java.nio.file.Paths

import com.bwsw.tstreamstransactionserver.netty.server.RocksDBWrapper
import com.bwsw.tstreamstransactionserver.netty.server.db.{DbMeta, KeyValueDbManager}
import com.bwsw.tstreamstransactionserver.netty.server.storage.rocks.RocksCompactionJob
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.{RocksStorageOptions, StorageOptions}
import org.apache.commons.io.FileUtils
import org.rocksdb.ColumnFamilyOptions

class RocksDbManager(storageOpts: StorageOptions,
                     rocksStorageOpts: RocksStorageOptions,
                     descriptors: Seq[RocksDbDescriptor],
                     readOnly: Boolean = false)
  extends KeyValueDbManager {
  private val path = Paths.get(storageOpts.path, storageOpts.metadataDirectory).toString
  private val file = new File(path)
  require(file.isAbsolute, "A parameter 'path' is incorrect. Path should be absolute. " +
    "For more info see storage settings description.")

  private val options = rocksStorageOpts.createDBOptions()

  private[rocks] val (client, databaseHandlers) = {
    val defaultDescriptor = new RocksDbDescriptor(
      DbMeta("default"),
      new ColumnFamilyOptions())
    val descriptorsWithDefaultDescriptor = defaultDescriptor +: descriptors

    FileUtils.forceMkdir(file)

    val (connection, columnFamilies) = RocksDBWrapper(
      options,
      file.getAbsolutePath,
      descriptorsWithDefaultDescriptor,
      readOnly)

    val handlerToIndexMap = columnFamilies
      .map { case RocksDBWrapper.ColumnFamily(descriptor, handle) => (descriptor.id, handle) }
      .toMap

    (connection, handlerToIndexMap)
  }


  private val maybeCompactionJob =
    if (readOnly) None
    else Some(new RocksCompactionJob(
      client,
      databaseHandlers.values.toSeq,
      storageOpts.dataCompactionInterval))

  maybeCompactionJob.foreach(_.start())


  def getDatabase(index: Int): RocksDb =
    new RocksDb(client, databaseHandlers(index))

  def newBatch: RocksDbBatch =
    new RocksDbBatch(client, databaseHandlers)

  override def closeDatabases(): Unit = {
    maybeCompactionJob.foreach(_.close())
    client.close()
  }
}
