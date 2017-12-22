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
