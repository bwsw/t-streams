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
import org.apache.curator.framework.recipes.cache.NodeCache


class RootNode(client: CuratorFramework,
               rootPath: String) {

  private val nodeCache =
    new NodeCache(client, rootPath, false)
  nodeCache.start()


  final def getCurrentData: RootNodeData = {
    nodeCache.rebuild()
    getLocalCachedCurrentData
  }

  final def getLocalCachedCurrentData: RootNodeData = {
    Option(nodeCache.getCurrentData)
      .map { node =>
        RootNodeData.fromByteArray(node.getData)
      }
      .getOrElse(
        RootNodeData(
          Array.emptyByteArray,
          Array.emptyByteArray
        ))
  }

  final def setFirstAndLastIDInRootNode(first: Array[Byte],
                                        second: Array[Byte]): Unit = {
    val nodeData =
      RootNodeData(first, second)
        .toByteArray

    client.setData()
      .forPath(rootPath, nodeData)
  }
}
