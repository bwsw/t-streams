package com.bwsw.tstreams.metadata

import com.datastax.driver.core.{PreparedStatement, Session}

import scala.collection.mutable

/**
  * Created by Ivan Kudryavtsev on 24.09.16.
  */
object RequestsRepository {
  val requestsMap = mutable.Map[Session, RequestsStatements]()
  def getStatements(session: Session): RequestsStatements = this.synchronized {
    if (requestsMap.contains(session))
      requestsMap(session)
    else {
      val statements = RequestsStatements.prepare(session)
      requestsMap(session) = statements
      statements
    }
  }
}

object RequestsStatements {
  def prepare(session: Session): RequestsStatements = {

    val commitLogTable  = "commit_log"
    val streamTable     = "streams"
    val consumerTable   = "consumers"

    val commitLogPutStatement = session.prepare(s"INSERT INTO $commitLogTable (stream, partition, interval, transaction, count) " +
      "VALUES (?, ?, ?, ?, ?) USING TTL ?")

    val scanStatement = s"SELECT stream, partition, interval, transaction, count, TTL(count) FROM $commitLogTable " +
      "WHERE stream = ? AND partition = ? AND interval = ?"

    val commitLogScanStatement    = session.prepare(scanStatement)
    val commitLogGetStatement     = session.prepare(s"$scanStatement AND transaction = ?")
    val commitLogDeleteStatement  = session.prepare(s"DELETE FROM $commitLogTable WHERE stream = ? AND partition = ? AND interval = ? AND transaction = ?")

    val streamInsertStatement = session.prepare(s"INSERT INTO $streamTable (stream_name, partitions, ttl, description) VALUES (?,?,?,?)")
    val streamDeleteStatement = session.prepare(s"DELETE FROM $streamTable WHERE stream_name=?")
    val streamSelectStatement = session.prepare(s"SELECT * FROM $streamTable WHERE stream_name=? LIMIT 1")

    val consumerCheckpointStatement = session.prepare(s"INSERT INTO $consumerTable (name, stream, partition, last_transaction) VALUES(?, ?, ?,  ?)")

    RequestsStatements(
      commitLogPutStatement       = commitLogPutStatement,
      commitLogGetStatement       = commitLogGetStatement,
      commitLogScanStatement      = commitLogScanStatement,
      commitLogDeleteStatement    = commitLogDeleteStatement,
      streamInsertStatement       = streamInsertStatement,
      streamDeleteStatement       = streamDeleteStatement,
      streamSelectStatement       = streamSelectStatement,
      consumerCheckpointStatement = consumerCheckpointStatement)
  }
}

case class RequestsStatements(commitLogPutStatement: PreparedStatement,
                              commitLogGetStatement: PreparedStatement,
                              commitLogDeleteStatement: PreparedStatement,
                              commitLogScanStatement: PreparedStatement,
                              streamInsertStatement: PreparedStatement,
                              streamDeleteStatement: PreparedStatement,
                              streamSelectStatement: PreparedStatement,
                              consumerCheckpointStatement: PreparedStatement)