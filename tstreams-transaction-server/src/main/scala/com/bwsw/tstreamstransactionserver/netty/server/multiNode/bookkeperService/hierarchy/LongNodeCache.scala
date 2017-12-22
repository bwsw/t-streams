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
