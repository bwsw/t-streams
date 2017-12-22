package com.bwsw.tstreamstransactionserver.netty.client

import com.bwsw.tstreamstransactionserver.netty.SocketHostPortPair

trait MasterReelectionListener {
  def masterChanged(newMaster: Either[Throwable, Option[SocketHostPortPair]])
}
