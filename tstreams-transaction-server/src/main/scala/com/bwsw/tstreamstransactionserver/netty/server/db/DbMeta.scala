package com.bwsw.tstreamstransactionserver.netty.server.db

case class DbMeta(name: String) {
  val binaryName: Array[Byte] = name.getBytes()
  val id: Int = java.util.Arrays.hashCode(binaryName)
}
