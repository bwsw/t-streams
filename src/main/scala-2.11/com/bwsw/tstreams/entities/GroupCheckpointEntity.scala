package com.bwsw.tstreams.entities


import com.bwsw.tstreams.agents.group.{CheckpointInfo, ConsumerCheckpointInfo, ProducerCheckpointInfo}
import com.datastax.driver.core.{BatchStatement, Session}

/**
  * Entity for providing group agents commits
  *
  * @param consumerEntityName Consumer metadata table name
  * @param producerEntityName Producer metadata table name
  * @param session            C* session
  */
class GroupCheckpointEntity(consumerEntityName: String, producerEntityName: String, session: Session) {
  /**
    * Statement for saving consumer single offset
    */
  private val consumerCheckpointStatement = session
    .prepare(s"insert into $consumerEntityName (name,stream,partition,last_transaction) values(?,?,?,?)")

  /**
    * Statement using for saving producer offsets
    */
  private val producerCheckpointStatement = session
    .prepare(s"insert into $producerEntityName (stream,partition,transaction,cnt) values(?,?,?,?) USING TTL ?")

  /**
    * Group agents commit
    *
    * @param info Info to commit
    *             (used only for consumers now; producers is not atomic)
    */
  def groupCheckpoint(info: List[CheckpointInfo]): Unit = {
    val batchStatement = new BatchStatement()
    info foreach {
      case ConsumerCheckpointInfo(name, stream, partition, offset) =>
        batchStatement.add(consumerCheckpointStatement.bind(name, stream, new Integer(partition), new java.lang.Long(offset)))
      case ProducerCheckpointInfo(_, _, _, _, streamName, partition, transaction, totalCnt, ttl) =>
        batchStatement.add(producerCheckpointStatement.bind(streamName, new Integer(partition), new java.lang.Long(transaction), new Integer(totalCnt), new Integer(ttl)))
    }
    session.execute(batchStatement)
  }
}
