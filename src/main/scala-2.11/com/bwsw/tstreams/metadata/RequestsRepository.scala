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
    val activityTable = "commit_log_activity"
    val activityPutStatement = session.prepare(s"INSERT INTO $activityTable (stream, partition, activity) VALUES (?, ?, ?)")
    val activityGetStatement = session.prepare(s"SELECT stream, partition, activity FROM $activityTable WHERE stream = ? AND partition = ? AND activity = ?")

    val commitLog = "commit_log"
    val commitLogPutStatement = session.prepare(s"INSERT INTO $commitLog (stream, partition, activity_second, transaction, count) " +
      "VALUES (?, ?, ?, ?, ?) USING TTL ?")

    val scanStatement = s"SELECT stream, partition, activity_second, transaction, count, TTL(count) FROM $commitLog " +
      "WHERE stream = ? AND partition = ? AND activity_second = ?"

    val commitLogScanStatement = session.prepare(scanStatement)

    val commitLogGetStatement = session.prepare(s"$scanStatement AND transaction = ?")


    RequestsStatements(activityPutStatement = activityPutStatement, activityGetStatement = activityGetStatement,
      commitLogPutStatement = commitLogPutStatement, commitLogGetStatement = commitLogGetStatement)
  }
}

case class RequestsStatements(activityPutStatement: PreparedStatement,
                              activityGetStatement: PreparedStatement,
                              commitLogPutStatement: PreparedStatement,
                              commitLogGetStatement: PreparedStatement)