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

import com.bwsw.tstreamstransactionserver.netty.server.db.DbMeta
import com.bwsw.tstreamstransactionserver.netty.server.db.rocks.RocksDbDescriptor._
import org.rocksdb._

case class RocksDbDescriptor(rocksDatabaseMeta: DbMeta,
                             options: ColumnFamilyOptions,
                             ttl: Integer = NoTTL) {
  def this(name: String,
           options: ColumnFamilyOptions,
           ttl: Integer) = {
    this(
      DbMeta(
        name
      ),
      options,
      ttl
    )
  }

  def id: Int = rocksDatabaseMeta.id

  def name: Array[Byte] = rocksDatabaseMeta.binaryName
}


object RocksDbDescriptor {
  val NoTTL: Integer = int2Integer(-1)
}
