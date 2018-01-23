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

import com.bwsw.tstreamstransactionserver.netty.server.CompactionJob
import com.bwsw.tstreamstransactionserver.options.CommonOptions
import org.rocksdb.{ColumnFamilyHandle, RocksDB}

import scala.concurrent.duration.TimeUnit

/**
  * Periodically runs RocksDB compaction.
  * It's need to delete expired entries from RocksDB (i.e. data and metadata of transactions).
  *
  * @param rocksDB  RocksDB database
  * @param handlers handler for RocksDB column family
  * @param interval time interval between compactions
  * @param timeUnit time unit of time interval
  * @author Pavel Tomskikh
  */
class RocksCompactionJob(rocksDB: RocksDB,
                         handlers: Seq[ColumnFamilyHandle],
                         interval: Long,
                         timeUnit: TimeUnit = CommonOptions.COMPACTION_TIME_UNIT)
  extends CompactionJob(interval, timeUnit) {

  override def compact(): Unit = {
    rocksDB.compactRange()
    handlers.foreach(rocksDB.compactRange)
  }
}
