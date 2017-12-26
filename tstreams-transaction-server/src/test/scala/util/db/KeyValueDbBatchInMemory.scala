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

import java.util.concurrent.atomic.AtomicLong

import com.bwsw.tstreamstransactionserver.netty.server.db.{KeyValueDb, KeyValueDbBatch}

import scala.collection.mutable.ArrayBuffer

class KeyValueDbBatchInMemory(dbs: Array[KeyValueDb])
  extends KeyValueDbBatch()
{
  private val operationBuffer = ArrayBuffer.empty[Unit => Unit]

  override def put(index: Int, key: Array[Byte], data: Array[Byte]): Boolean =
    this.synchronized {
      val operation: Unit => Unit =
        Unit => dbs(index).put(key, data)
      operationBuffer += operation
      true
    }

  override def remove(index: Int, key: Array[Byte]): Unit =
    this.synchronized {
      val operation: Unit => Unit =
        Unit => dbs(index).delete(key)
      operationBuffer += operation
    }

  override def write(): Boolean = {
    operationBuffer.foreach(operation =>
      operation(())
    )
    true
  }
}
