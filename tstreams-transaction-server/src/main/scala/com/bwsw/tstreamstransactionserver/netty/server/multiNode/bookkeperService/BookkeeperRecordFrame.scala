package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService

import com.bwsw.tstreamstransactionserver.netty.server.batch.Frame
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.data.Record

class BookkeeperRecordFrame(record: Record)
  extends Frame (
    record.recordType,
    record.timestamp,
    record.body
  )
