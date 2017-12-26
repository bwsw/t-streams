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

package util.db

import com.bwsw.tstreamstransactionserver.netty.server.db.{KeyValueDb, KeyValueDbIterator}

import scala.collection.concurrent.TrieMap

class KeyValueDbInMemory
  extends KeyValueDb {
  private val db = new TrieMap[Array[Byte], Array[Byte]]()

  override def get(key: Array[Byte]): Array[Byte] =
    db.getOrElse(key, null)

  override def put(key: Array[Byte], data: Array[Byte]): Boolean = {
    db.put(key, data)
    true
  }

  override def delete(key: Array[Byte]): Boolean = {
    Option(db.remove(key)).isDefined
  }

  override def iterator: KeyValueDbIterator = ???
}
