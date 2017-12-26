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

package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.{NodeCache, NodeCacheListener}

class LongNodeCache(client: CuratorFramework,
                    path: String)
  extends NodeCacheListener {
  @volatile private var currentId = -1L

  private val nodeCache = {
    val node = new NodeCache(
      client,
      path
    )
    node
      .getListenable
      .addListener(this)
    node
  }

  private def getCachedCurrentId: Long = {
    val binaryIdOpt = Option(
      nodeCache.getCurrentData
    )

    binaryIdOpt
      .map(_.getData)
      .filter(_.nonEmpty)
      .map { binaryId =>
        java.nio.ByteBuffer
          .wrap(binaryId)
          .getLong
      }.getOrElse(-1L)
  }

  def startMonitor(): Unit = {
    nodeCache.start(true)
    currentId = getCachedCurrentId
  }

  def stopMonitor(): Unit =
    nodeCache.close()

  def getId: Long =
    currentId

  override def nodeChanged(): Unit = {
    currentId = getCachedCurrentId
  }
}
