package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService


class BookkeeperMasterBundle(val bookkeeperMaster: BookkeeperMaster) {
  private val masterTask =
    new Thread(
      bookkeeperMaster,
      "bookkeeper-master-%d"
    )

  def start(): Unit = {
    masterTask.start()
  }


  def stop(): Unit = {
    masterTask.interrupt()
  }
}
