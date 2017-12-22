package com.bwsw.tstreamstransactionserver.netty.server.multiNode.common

import com.bwsw.tstreamstransactionserver.netty.server.RocksWriter
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy.{LongNodeCache, LongZookeeperTreeList}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService._
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.commitLogService.CommitLogService
import com.bwsw.tstreamstransactionserver.netty.server.zk.{ZKIDGenerator, ZKMasterElector}
import com.bwsw.tstreamstransactionserver.options.MultiNodeServerOptions.{BookkeeperOptions, CommonPrefixesOptions}
import org.apache.curator.framework.CuratorFramework

class CommonBookkeeperWriter(zookeeperClient: CuratorFramework,
                             bookkeeperOptions: BookkeeperOptions,
                             commonPrefixesOptions: CommonPrefixesOptions)
  extends BookkeeperWriter(
    zookeeperClient,
    bookkeeperOptions) {

  private val commonMasterZkTreeList =
    new LongZookeeperTreeList(
      zookeeperClient,
      commonPrefixesOptions.commonMasterZkTreeListPrefix
    )

  private val checkpointMasterZkTreeList =
    new LongZookeeperTreeList(
      zookeeperClient,
      commonPrefixesOptions.checkpointGroupPrefixesOptions.checkpointGroupZkTreeListPrefix
    )

  private val commonMasterLastClosedLedger =
    new LongNodeCache(
      zookeeperClient,
      commonPrefixesOptions
        .commonMasterLastClosedLedger
    )

  private val checkpointMasterLastClosedLedger =
    new LongNodeCache(
      zookeeperClient,
      commonPrefixesOptions
        .checkpointGroupPrefixesOptions
        .checkpointGroupLastClosedLedger
    )

  private val zkTreesList =
    Array(commonMasterZkTreeList, checkpointMasterZkTreeList)

  private val lastClosedLedgerHandlers =
    Array(commonMasterLastClosedLedger, checkpointMasterLastClosedLedger)
  lastClosedLedgerHandlers.foreach(_.startMonitor())


  override def getLastConstructedLedger: Long = {
    val ledgerIds =
      for {
        zkTree <- zkTreesList
        lastConstructedLedgerId <- zkTree.lastEntityID
      } yield lastConstructedLedgerId

    if (ledgerIds.isEmpty) {
      -1L
    } else {
      ledgerIds.max
    }
  }

  def createCommonMaster(zKMasterElector: ZKMasterElector,
                         zkLastClosedLedgerHandler: ZKIDGenerator): BookkeeperMasterBundle = {
    createMaster(
      zKMasterElector,
      zkLastClosedLedgerHandler,
      commonPrefixesOptions.timeBetweenCreationOfLedgersMs,
      commonMasterZkTreeList
    )
  }

  def createSlave(commitLogService: CommitLogService,
                  rocksWriter: RocksWriter): BookkeeperSlaveBundle = {
    val bookkeeperSlave =
      new BookkeeperSlave(
        bookKeeper,
        bookkeeperOptions,
        zkTreesList,
        lastClosedLedgerHandlers,
        commitLogService,
        rocksWriter,
      )

    val timeBetweenCreationOfLedgersMs = math.max(
      commonPrefixesOptions
        .checkpointGroupPrefixesOptions
        .timeBetweenCreationOfLedgersMs,
      commonPrefixesOptions
        .timeBetweenCreationOfLedgersMs
    )

    new BookkeeperSlaveBundle(
      bookkeeperSlave,
      lastClosedLedgerHandlers,
      timeBetweenCreationOfLedgersMs
    )
  }

}
