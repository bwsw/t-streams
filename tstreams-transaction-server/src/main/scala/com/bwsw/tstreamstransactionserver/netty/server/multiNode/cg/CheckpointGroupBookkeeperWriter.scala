package com.bwsw.tstreamstransactionserver.netty.server.multiNode.cg

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy.LongZookeeperTreeList
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.{BookkeeperMasterBundle, BookkeeperWriter}
import com.bwsw.tstreamstransactionserver.netty.server.zk.{ZKIDGenerator, ZKMasterElector}
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.{BookkeeperOptions, CheckpointGroupPrefixesOptions}
import org.apache.curator.framework.CuratorFramework

class CheckpointGroupBookkeeperWriter(zookeeperClient: CuratorFramework,
                                      bookkeeperOptions: BookkeeperOptions,
                                      checkpointGroupPrefixesOptions: CheckpointGroupPrefixesOptions)
  extends BookkeeperWriter(
    zookeeperClient,
    bookkeeperOptions
  ) {

  private val checkpointMasterZkTreeList =
    new LongZookeeperTreeList(
      zookeeperClient,
      checkpointGroupPrefixesOptions.checkpointGroupZkTreeListPrefix
    )

  override def getLastConstructedLedger: Long = {
    checkpointMasterZkTreeList
      .lastEntityID
      .getOrElse(-1L)
  }

  def createCheckpointMaster(zKMasterElector: ZKMasterElector,
                             zkLastClosedLedgerHandler: ZKIDGenerator): BookkeeperMasterBundle = {
    createMaster(
      zKMasterElector,
      zkLastClosedLedgerHandler,
      checkpointGroupPrefixesOptions.timeBetweenCreationOfLedgersMs,
      checkpointMasterZkTreeList
    )
  }

}
