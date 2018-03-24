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
package com.bwsw.tstreamstransactionserver.netty.server

import java.util
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import com.bwsw.tstreamstransactionserver.netty.server.db.rocks.{RocksDbDescriptor, RocksDbIteratorWrapper}
import org.rocksdb._
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.{Failure, Success, Try}
import scala.collection.JavaConverters._

/**
  * Wrapper for [[RocksDB]]. Used to avoid access for closed [[RocksDB]].
  */
class RocksDBWrapper private(path: String,
                             rocksDB: RocksDB,
                             readOnly: Boolean) {

  private val isClosed = new AtomicBoolean(false)
  private val compactionLock = new ReentrantLock()
  private val logger = LoggerFactory.getLogger(classOf[RocksDBWrapper])

  if (logger.isInfoEnabled) {
    logger.info(s"RocksDB at path '$path' is started.")
  }

  def get(key: Array[Byte]): Array[Byte] = {
    throwIfClosed()
    rocksDB.get(key)
  }

  def get(columnFamilyHandle: ColumnFamilyHandle, key: Array[Byte]): Array[Byte] = {
    throwIfClosed()
    rocksDB.get(columnFamilyHandle, key)
  }

  def put(key: Array[Byte], value: Array[Byte]): Unit = {
    throwIfClosed()
    rocksDB.put(key, value)
  }

  def put(columnFamilyHandle: ColumnFamilyHandle, key: Array[Byte], value: Array[Byte]): Unit = {
    throwIfClosed()
    rocksDB.put(columnFamilyHandle, key, value)
  }

  def delete(key: Array[Byte]): Unit = {
    throwIfClosed()
    rocksDB.delete(key)
  }

  def delete(columnFamilyHandle: ColumnFamilyHandle, key: Array[Byte]): Unit = {
    throwIfClosed()
    rocksDB.delete(columnFamilyHandle, key)
  }

  def newIterator(): RocksDbIteratorWrapper = {
    throwIfClosed()
    new RocksDbIteratorWrapper(
      rocksDB.newIterator(),
      isClosed.get())
  }

  def newIterator(columnFamilyHandle: ColumnFamilyHandle): RocksDbIteratorWrapper = {
    throwIfClosed()
    new RocksDbIteratorWrapper(
      rocksDB.newIterator(columnFamilyHandle),
      isClosed.get())
  }

  def write(writeOptions: WriteOptions, batch: WriteBatch): Unit = {
    throwIfClosed()
    rocksDB.write(writeOptions, batch)
  }


  def compactRange(): Unit =
    withCompactionLock(rocksDB.compactRange())

  def compactRange(columnFamilyHandle: ColumnFamilyHandle): Unit =
    withCompactionLock(rocksDB.compactRange(columnFamilyHandle))


  def close(): Unit = {
    if (!isClosed.getAndSet(true)) {
      compactionLock.lock()
      val result = Try(rocksDB.close())
      compactionLock.unlock()
      if (logger.isInfoEnabled) {
        logger.info(s"RocksDB at path '$path' is closed.")
      }

      result match {
        case Success(_) if !readOnly => RocksDBWrapper.remove(path)
        case Success(_) =>
        case Failure(exception) => throw exception
      }
    } else {
      throw new IllegalStateException(s"RocksDB at path '$path' is closed already.")
    }
  }


  private def throwIfClosed(): Unit = {
    if (isClosed.get()) {
      throw new IllegalStateException(s"RocksDB at path '$path' is closed.")
    }
  }

  private def withCompactionLock(compaction: => Unit): Unit = {
    compactionLock.lock()
    val result = Try {
      throwIfClosed()
      compaction
    }
    compactionLock.unlock()

    result match {
      case Success(_) =>
      case Failure(exception) => throw exception
    }
  }
}


object RocksDBWrapper {

  private val openedRocksDBs = mutable.Set.empty[String]
  RocksDB.loadLibrary()

  /**
    * Opens [[RocksDBWrapper]] instance
    */
  def apply(options: Options,
            path: String,
            ttl: Int,
            readOnly: Boolean): RocksDBWrapper = {
    open(
      path,
      readOnly,
      open(options, path, ttl, readOnly))
  }

  /**
    * Opens [[RocksDBWrapper]] instance
    */
  def apply(options: DBOptions,
            path: String,
            rocksDbDescriptors: Seq[RocksDbDescriptor],
            readOnly: Boolean): (RocksDBWrapper, Seq[ColumnFamily]) = {
    open(
      path,
      readOnly,
      open(options, path, rocksDbDescriptors, readOnly))
  }


  private def open[T](path: String,
                      readOnly: Boolean,
                      constructor: => T): T = synchronized {
    if (readOnly) {
      constructor
    } else if (openedRocksDBs.contains(path)) {
      throw new IllegalArgumentException(s"RocksDB already opened at path '$path' with read-write mode.")
    } else {
      val rocksDB = constructor
      openedRocksDBs += path

      rocksDB
    }
  }

  private def open(options: Options,
                   path: String,
                   ttl: Int,
                   readOnly: Boolean) = {
    val rocksDB = TtlDB.open(
      options,
      path,
      ttl,
      readOnly)

    new RocksDBWrapper(path, rocksDB, readOnly)
  }

  private def open(options: DBOptions,
                   path: String,
                   rocksDbDescriptors: Seq[RocksDbDescriptor],
                   readOnly: Boolean) = {
    val columnFamilyDescriptors = rocksDbDescriptors.map { descriptor =>
      new ColumnFamilyDescriptor(descriptor.name, descriptor.options)
    }.asJava
    val ttlValues = rocksDbDescriptors.map(param => Int.box(param.ttl)).asJava
    val columnFamilyHandles = new util.ArrayList[ColumnFamilyHandle](rocksDbDescriptors.length)

    val rocksDB = TtlDB.open(
      options,
      path,
      columnFamilyDescriptors,
      columnFamilyHandles,
      ttlValues,
      readOnly)

    val columnFamilies = columnFamilyHandles.asScala.zip(rocksDbDescriptors)
      .map { case (handle, descriptor) => ColumnFamily(descriptor, handle) }

    (new RocksDBWrapper(path, rocksDB, readOnly), columnFamilies)
  }

  private def remove(path: String): Unit =
    synchronized(openedRocksDBs -= path)


  final case class ColumnFamily(descriptor: RocksDbDescriptor,
                                handle: ColumnFamilyHandle)

}
