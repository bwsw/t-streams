package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy

trait EntityPathConverter[T] {
  def entityToPath(entity: T): Array[String]
}
