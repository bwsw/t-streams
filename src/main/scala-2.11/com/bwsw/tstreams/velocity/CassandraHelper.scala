package com.bwsw.tstreams.velocity

import com.datastax.driver.core.Session

/**
 * Test util for creating C* entities
 */
object CassandraHelper {

  /**
   * Keyspace creator helper
   * @param session Session instance which will be used for keyspace creation
   * @param keyspace Keyspace name
   */
  def createKeyspace(session: Session, keyspace : String) = session.execute(s"CREATE KEYSPACE $keyspace WITH replication = " +
                                                                  s" {'class': 'SimpleStrategy', 'replication_factor': '1'} " +
                                                                  s" AND durable_writes = true")

  /**
   * Metadata tables creator helper
   * @param session Session
   * @param keyspace Keyspace name
   */
  def createMetadataTables(session : Session, keyspace : String) = {
    
    session.execute(s"CREATE TABLE $keyspace.stream_commit_last (" +
      s"stream text, " +
      s"partition int, " +
      s"transaction timeuuid, " +
      s"PRIMARY KEY (stream, partition))")
    
    session.execute(s"CREATE TABLE $keyspace.consumers (" +
      s"name text, " +
      s"stream text, " +
      s"partition int, " +
      s"last_transaction timeuuid, " +
      s"PRIMARY KEY (name, stream, partition))")

    
    session.execute(s"CREATE TABLE $keyspace.streams (" +
      s"stream_name text PRIMARY KEY, " +
      s"partitions int," +
      s"ttl int, " +
      s"description text)")

    
    session.execute(s"CREATE TABLE $keyspace.commit_log (" +
      s"stream text, " +
      s"partition int, " +
      s"transaction timeuuid, " +
      s"cnt int, " +
      s"PRIMARY KEY (stream, partition, transaction))")

    
    session.execute(s"CREATE TABLE $keyspace.generators (" +
      s"name text, " +
      s"time timeuuid, " +
      s"PRIMARY KEY (name))")
    
  }

  /**
   * Cassandra data table creator helper
   * @param session Session
   * @param keyspace Keyspace name
   */
  def createDataTable(session: Session, keyspace: String) = {

    session.execute(s"CREATE TABLE $keyspace.data_queue ( " +
      s"stream text, " +
      s"partition int, " +
      s"transaction timeuuid, " +
      s"seq int, " +
      s"data blob, " +
      s"PRIMARY KEY ((stream, partition), transaction, seq))")
  }

  /**
   * Cassandra storage table dropper helper
   * @param session Session
   * @param keyspace Keyspace name
   */
  def dropDataTable(session: Session, keyspace : String) = {
    session.execute(s"DROP TABLE $keyspace.data_queue")
  }

  /**
   * Cassandra metadata storage table dropper helper
   * @param session Session
   * @param keyspace Keyspace name
   */
  def dropMetadataTables(session: Session, keyspace : String) = {
    session.execute(s"DROP TABLE $keyspace.stream_commit_last")

    session.execute(s"DROP TABLE $keyspace.consumers")

    session.execute(s"DROP TABLE $keyspace.streams")

    session.execute(s"DROP TABLE $keyspace.commit_log")

    session.execute(s"DROP TABLE $keyspace.generators")
  }

  /**
   * Metadata table flushing helper
   * @param session Session
   * @param keyspace Keyspace name
   */
  def clearMetadataTables(session: Session, keyspace : String) = {
    dropMetadataTables(session, keyspace)
    createMetadataTables(session, keyspace)
  }


  /**
   * Cassandra data table creator helper
   * @param session Session
   * @param keyspace Keyspace name
   */
  def clearDataTable(session: Session, keyspace : String) = {
    dropDataTable(session, keyspace)
    createDataTable(session, keyspace)
  }
}