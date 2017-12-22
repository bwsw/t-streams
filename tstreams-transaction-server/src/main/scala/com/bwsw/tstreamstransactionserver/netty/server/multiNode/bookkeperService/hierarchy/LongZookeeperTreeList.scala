package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy

import org.apache.curator.framework.CuratorFramework

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

  override def entityIDtoBytes(entity: Long): Array[Byte] = {
    val size = java.lang.Long.BYTES
    val buffer = java.nio.ByteBuffer.allocate(size)

    buffer.putLong(entity)
    buffer.flip()

    val bytes = new Array[Byte](size)
    buffer.get(bytes)
    bytes
  }

  override def bytesToEntityID(bytes: Array[Byte]): Long = {
    val buffer = java.nio.ByteBuffer.wrap(bytes)
    buffer.getLong()
  }
}
