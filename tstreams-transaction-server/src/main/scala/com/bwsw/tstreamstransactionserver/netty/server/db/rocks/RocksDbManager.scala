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
import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.netty.server.db.{DbMeta, KeyValueDbManager}
import com.bwsw.tstreamstransactionserver.netty.server.storage.rocks.RocksCompactionJob
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.RocksStorageOptions
import org.apache.commons.io.FileUtils
import org.rocksdb._

import scala.collection.JavaConverters

class RocksDbManager(rocksStorageOpts: RocksStorageOptions,
                     absolutePath: String,
                     descriptors: Seq[RocksDbDescriptor],
                     compactionInterval: Long,
                     readOnly: Boolean = false)
  extends KeyValueDbManager {
  RocksDB.loadLibrary()

  private val options = rocksStorageOpts.createDBOptions()

  private[rocks] val (client, descriptorsWorkWith, databaseHandlers) = {
    val descriptorsWithDefaultDescriptor =
      new RocksDbDescriptor(
        DbMeta("default"),
        new ColumnFamilyOptions()
      ) +: descriptors

    val (columnFamilyDescriptors, ttls) = descriptorsWithDefaultDescriptor
      .map(descriptor =>
        (
          new ColumnFamilyDescriptor(descriptor.name, descriptor.options),
          descriptor.ttl
        )
      ).unzip

    val databaseHandlers =
      new java.util.ArrayList[ColumnFamilyHandle](columnFamilyDescriptors.length)

    val file = new File(absolutePath)
    FileUtils.forceMkdir(file)

    val connection = TtlDB.open(
      options,
      file.getAbsolutePath,
      JavaConverters.seqAsJavaList(columnFamilyDescriptors),
      databaseHandlers,
      JavaConverters.seqAsJavaList(ttls),
      readOnly
    )

    val handlerToIndexMap: collection.immutable.Map[Int, ColumnFamilyHandle] =
      JavaConverters.asScalaBuffer(databaseHandlers)
        .zip(descriptorsWithDefaultDescriptor)
        .map { case (handler, descriptor) => (descriptor.id, handler) }
        .toMap

    (
      connection,
      descriptorsWithDefaultDescriptor.toBuffer,
      handlerToIndexMap
    )
  }


  private val maybeCompactionJob =
    if (readOnly) None
    else Some(new RocksCompactionJob(
      client,
      databaseHandlers.values.toSeq,
      compactionInterval))

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
