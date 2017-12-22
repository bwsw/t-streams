package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

trait LeaderSelectorInterface {
  def hasLeadership: Boolean

  def stopParticipateInElection(): Unit
}
