package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy

case class PersistedCommitAndMoveToNextRecordsInfo(isCommitted: Boolean,
                                                   doReadNextRecords: Boolean)
