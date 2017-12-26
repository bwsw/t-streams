package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

import java.util.concurrent.atomic.AtomicInteger

class BookkeeperMasterBundle(val bookkeeperMaster: BookkeeperMaster) {

  private val masterTask =
    new Thread(
      bookkeeperMaster,
      s"bookkeeper-master-${BookkeeperMasterBundle.threadIndex.getAndIncrement()}"
    )

  def start(): Unit = {
    masterTask.start()
  }


  def stop(): Unit = {
    masterTask.interrupt()
  }
}

object BookkeeperMasterBundle {
  private val threadIndex = new AtomicInteger(0)
}