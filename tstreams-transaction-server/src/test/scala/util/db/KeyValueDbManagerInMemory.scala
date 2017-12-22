package util.db

import com.bwsw.tstreamstransactionserver.netty.server.db.{KeyValueDb, KeyValueDbBatch, KeyValueDbManager}

class KeyValueDbManagerInMemory(dbs: Array[KeyValueDb])
  extends KeyValueDbManager {

  override def getDatabase(index: Int): KeyValueDb =
    dbs(index)

  override def newBatch: KeyValueDbBatch =
    new KeyValueDbBatchInMemory(dbs)

  override def closeDatabases(): Unit = () //nothing
}
