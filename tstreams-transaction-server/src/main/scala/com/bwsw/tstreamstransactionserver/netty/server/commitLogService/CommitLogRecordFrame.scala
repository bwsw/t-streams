package com.bwsw.tstreamstransactionserver.netty.server.commitLogService

import com.bwsw.commitlog.CommitLogRecord
import com.bwsw.tstreamstransactionserver.netty.server.batch.Frame

class CommitLogRecordFrame(record: CommitLogRecord)
  extends Frame (
    record.messageType,
    record.timestamp,
    record.message
  )

