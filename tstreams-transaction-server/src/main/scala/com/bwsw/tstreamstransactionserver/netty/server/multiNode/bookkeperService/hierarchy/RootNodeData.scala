package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy


object RootNodeData {
  val delimiterIndexFieldSize: Int = java.lang.Integer.BYTES

  def fromByteArray(bytes: Array[Byte]): RootNodeData = {

    if (bytes.isEmpty) {
      new RootNodeData(
        Array.emptyByteArray,
        Array.emptyByteArray
      )
    }
    else {
      val buf = java.nio.ByteBuffer
        .wrap(bytes)

      val firstSize = buf.getInt
      val first = new Array[Byte](firstSize)
      buf.get(first)

      val second = new Array[Byte](buf.remaining())
      buf.get(second)

      new RootNodeData(
        first,
        second
      )
    }
  }

  def apply(firstID: Array[Byte],
            lastID: Array[Byte]): RootNodeData =
    new RootNodeData(firstID, lastID)
}


class RootNodeData(val firstID: Array[Byte],
                   val lastID: Array[Byte]) {
  def toByteArray: Array[Byte] = {
    val size =
      RootNodeData.delimiterIndexFieldSize +
        firstID.length +
        lastID.length

    val buf = java.nio.ByteBuffer
      .allocate(size)
      .putInt(firstID.length)
      .put(firstID)
      .put(lastID)
    buf.flip()

    if (buf.hasArray) {
      buf.array()
    }
    else {
      val bytes = new Array[Byte](size)
      buf.get(bytes)
      bytes
    }
  }

  override def hashCode(): Int = {
    val firstIdHash =
      java.util.Arrays.hashCode(firstID)

    val lastIdHash =
      java.util.Arrays.hashCode(lastID)

    31 * (
      31 + firstIdHash.hashCode()
      ) + lastIdHash.hashCode()
  }

  override def equals(o: scala.Any): Boolean = o match {
    case that: RootNodeData =>
      this.firstID.sameElements(that.firstID) &&
        this.lastID.sameElements(that.lastID)
    case _ => false
  }
}
