package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

import com.bwsw.tstreamstransactionserver.netty.server.zk.ZKMasterElector


class LeaderSelector(zKMasterElector: ZKMasterElector)
  extends LeaderSelectorInterface {

  override def hasLeadership: Boolean =
    zKMasterElector.hasLeadership()

  override def stopParticipateInElection(): Unit =
    zKMasterElector.stop()
}
