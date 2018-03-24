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

import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDbIterator
import org.rocksdb.RocksIterator

final class RocksDbIteratorWrapper(iterator: RocksIterator,
                                   rocksDBIsClosed: => Boolean)
  extends KeyValueDbIterator {

  private val isClosed = new AtomicBoolean(false)

  override def key(): Array[Byte] = {
    throwIfClosed()
    iterator.key()
  }

  override def value(): Array[Byte] = {
    throwIfClosed()
    iterator.value()
  }

  override def isValid: Boolean = {
    throwIfClosed()
    iterator.isValid
  }

  override def seekToFirst(): Unit = {
    throwIfClosed()
    iterator.seekToFirst()
  }

  override def seekToLast(): Unit = {
    throwIfClosed()
    iterator.seekToLast()
  }

  override def seek(target: Array[Byte]): Unit = {
    throwIfClosed()
    iterator.seek(target)
  }

  override def next(): Unit = {
    throwIfClosed()
    iterator.next()
  }

  override def prev(): Unit = {
    throwIfClosed()
    iterator.prev()
  }


  override def close(): Unit = {
    if (!isClosed.getAndSet(true)) {
      if (rocksDBIsClosed) {
        throw new IllegalStateException("RocksDB is closed.")
      }
      iterator.close()
    } else {
      throw new IllegalStateException("RocksIterator is closed yet.")
    }
  }


  private def throwIfClosed(): Unit = {
    if (isClosed.get()) {
      throw new IllegalStateException("RocksIterator is closed.")
    } else if (rocksDBIsClosed) {
      throw new IllegalStateException("RocksDB is closed.")
    }
  }
}
