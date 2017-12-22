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

import java.io.{Closeable, File}

import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.RocksStorageOptions
import org.apache.commons.io.FileUtils
import org.rocksdb._

class RocksDbConnection(rocksStorageOpts: RocksStorageOptions,
                        absolutePath: String,
                        ttl: Int = -1,
                        readOnly: Boolean = false)
  extends Closeable {
  RocksDB.loadLibrary()

  private val options = rocksStorageOpts.createOptions()
  private val file = new File(absolutePath)
  private val client = {
    FileUtils.forceMkdir(file)
    TtlDB.open(options, file.getAbsolutePath, ttl, readOnly)
  }


  def get(key: Array[Byte]) = client.get(key)

  @throws[RocksDBException]
  def put(key: Array[Byte], data: Array[Byte]): Unit = client.put(key, data)

  def getLastRecord: Option[(Array[Byte], Array[Byte])] = {
    val iterator = client.newIterator()
    iterator.seekToLast()
    val record = if (iterator.isValid) {
      val keyValue = (iterator.key(), iterator.value())
      Some(keyValue)
    }
    else {
      None
    }
    iterator.close()
    record
  }

  def iterator: RocksIterator = client.newIterator()

  override def close(): Unit = client.close()

  final def closeAndDeleteFolder(): Unit = {
    options.close()
    client.close()
    file.delete()
  }

  def newBatch = new Batch

  class Batch() {
    private val batch = new WriteBatch()

    def put(key: Array[Byte], data: Array[Byte]): Unit = {
      batch.put(key, data)
    }

    def remove(key: Array[Byte]): Unit = batch.remove(key)

    def write(): Boolean = {
      val writeOptions = new WriteOptions()
      val status = scala.util.Try(client.write(writeOptions, batch)) match {
        case scala.util.Success(_) => true
        case scala.util.Failure(throwable) =>
          throwable.printStackTrace()
          false
      }
      writeOptions.close()
      batch.close()
      status
    }
  }

  //  def newFileWriter = new FileWriter
  //  class FileWriter {
  //    private val sstFileWriter = new SstFileWriter(new EnvOptions(), options, RocksDbConnection.comparator)
  //    private val fileNew = new File(file.getAbsolutePath, "sst_file.sst")
  //    sstFileWriter.open(fileNew.getAbsolutePath)
  //
  //    def putData(data: Array[Byte]): Unit = sstFileWriter.add(new Slice(data), new Slice(Array[Byte]()))
  //    def finish(): Unit = {
  //      sstFileWriter.finish()
  //      client.compactRange()
  //      client.addFileWithFilePath(fileNew.getAbsolutePath, true)
  //    }
  //  }
}

//private object RocksDbConnection extends App {
//  RocksDB.loadLibrary()
//  lazy val comparatorOptions = new ComparatorOptions()
//  lazy val comparator = new org.rocksdb.util.BytewiseComparator(comparatorOptions)
//}