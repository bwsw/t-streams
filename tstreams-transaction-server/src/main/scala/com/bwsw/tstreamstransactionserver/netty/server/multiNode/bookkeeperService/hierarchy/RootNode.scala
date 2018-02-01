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
  * Class allows managing data of znode. Data consists of two elements, each of which is set of bytes
  *
  * @param client   zookeeper client
  * @param rootPath node path
  */
class RootNode(client: CuratorFramework,
               rootPath: String) {
  private val emptyNodeData = RootNodeData().toByteArray

  Option(client.checkExists().forPath(rootPath)) match {
    case Some(_) =>
    case None => client.create.creatingParentsIfNeeded().forPath(rootPath, emptyNodeData)
  }

  final def getData(): RootNodeData = {
    Option(client.getData.forPath(rootPath))
      .map(RootNodeData.fromByteArray)
      .getOrElse(RootNodeData())
  }

  final def setData(firstId: Array[Byte],
                    lastId: Array[Byte]): Unit = {
    val nodeData = RootNodeData(firstId, lastId).toByteArray
    client.setData().forPath(rootPath, nodeData)
  }

  final def clear(): Unit = {
    client.setData().forPath(rootPath, emptyNodeData)
  }
}
