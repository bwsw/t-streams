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

package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.hierarchy

import org.apache.curator.framework.CuratorFramework

/**
  * Class manages entities meta in zookeeper using 4-level hierarchical znodes.
  *
  * It splits the id into 4 parts (2 bytes each) and convert each of parts to hex-digit.
  * Then these parts are used to store id as a hierarchical structure of znodes.
  *
  * @param client   zookeeper client
  * @param rootPath node path
  */
class LongZookeeperTreeList(client: CuratorFramework, rootPath: String)
  extends ZookeeperTreeList[Long](client, rootPath) {

  override def entityToPath(entity: Long): Array[String] = {
    def splitLongToHexes: Array[String] = {
      val size = java.lang.Long.BYTES
      val buffer = java.nio.ByteBuffer.allocate(
        size
      )
      buffer.putLong(entity)
      buffer.flip()

      val bytes = Array.fill(4)(
        f"${buffer.getShort & 0xffff}%x"
      )
      bytes
    }

    splitLongToHexes
  }

  override def entityIdToBytes(entity: Long): Array[Byte] = {
    val size = java.lang.Long.BYTES
    val buffer = java.nio.ByteBuffer.allocate(size)

    buffer.putLong(entity)
    buffer.flip()

    val bytes = new Array[Byte](size)
    buffer.get(bytes)
    bytes
  }

  override def bytesToEntityId(bytes: Array[Byte]): Long = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)
    buffer.getLong()
  }
}
