package com.bwsw.tstreams.streams

import com.bwsw.tstreams.data.IStorage
import com.bwsw.tstreams.metadata.{MetadataStorage, RequestsRepository}
import com.datastax.driver.core.Session

/**
  * Settings of stream in metadata storage
  *
  * @param name        Stream name
  * @param partitions  Number of stream partitions
  * @param ttl         Time in seconds of transaction expiration
  * @param description Some stream additional info
  */
case class StreamSettings(name: String, partitions: Int, ttl: Int, description: String)

object Stream {

  /**
    * Retrieving stream with concrete name
    *
    * @param name Stream name to fetch from database
    * @return StreamSettings
    */
  def getStream(session: Session, name: String): Option[StreamSettings] = {
    val requests = RequestsRepository.getStatements(session)
    val statementWithBindings = requests.streamSelectStatement.bind(name)
    val stream = session.execute(statementWithBindings).one()

    if (stream == null)
      None
    else {
      val name        = stream.getString("stream_name")
      val partitions  = stream.getInt("partitions")
      val description = stream.getString("description")
      val ttl         = stream.getInt("ttl")
      Some(StreamSettings(name, partitions, ttl, description))
    }
  }

  /**
    * Create stream with parameters
    *
    * @param name        Stream name to use (unique id)
    * @param partitions  Number of stream partitions
    * @param ttl         Amount of expiration time of transaction
    * @param description Stream arbitrary description and com.bwsw.tstreams.metadata, etc.
    * @return StreamSettings
    */
  def createStream(session: Session,
                   name: String,
                   partitions: Int,
                   ttl: Int,
                   description: String): Unit = {
    if (isExist(session, name))
      throw new IllegalArgumentException(s"Stream $name already exist")

    val requests  = RequestsRepository.getStatements(session)
    val values    = List(name, new Integer(partitions), new Integer(ttl), description)
    val statementWithBindings = requests.streamInsertStatement.bind(values: _*)

    session.execute(statementWithBindings)
  }

  /**
    * Alternate stream with parameters
    *
    * @param name        Stream name to use (unique id)
    * @param partitions  Number of stream partitions
    * @param ttl         Amount of expiration time of transaction
    * @param description Stream arbitrary description and com.bwsw.tstreams.metadata, etc.
    * @return StreamSettings
    */
  def changeStream(session: Session, name: String, partitions: Int, ttl: Int, description: String): Unit = {
    if (!isExist(session, name))
      throw new IllegalArgumentException(s"Stream $name does not exist.")

    val values                = List(name, new Integer(partitions), new Integer(ttl), description)
    val requests              = RequestsRepository.getStatements(session)
    val statementWithBindings = requests.streamInsertStatement.bind(values: _*)
    session.execute(statementWithBindings)
  }

  /**
    * Deleting concrete stream
    *
    * @param name Stream name to delete
    */
  def deleteStream(session: Session, name: String): Unit = {
    if (!isExist(session, name))
      throw new IllegalArgumentException("stream not exist")

    val requests              = RequestsRepository.getStatements(session)
    val statementWithBindings = requests.streamDeleteStatement.bind(name)
    session.execute(statementWithBindings)
  }

  /**
    * Checking that concrete stream exist
    *
    * @param name Stream name to check if exists
    * @return Exist stream or not
    */
  def isExist(session: Session, name: String): Boolean = {
    val checkVal = getStream(session, name).isDefined
    checkVal
  }
}

/**
  * @param name            Name of the stream
  * @param partitionsCount Number of stream partitions
  * @param metadataStorage Stream metadata storage which it used
  * @param dataStorage     Data storage which will be using stream
  * @param ttl             Time of transaction time expiration in seconds
  * @param description     Some additional info about stream
  * @tparam T Storage data type
  */
class Stream[T](val name: String,
                var partitionsCount: Int,
                val metadataStorage: MetadataStorage,
                val dataStorage: IStorage[T],
                var ttl: Int,
                var description: String) {
  /**
    * Transaction minimum ttl time
    */
  private val minTransactionTTL = 3

  if (ttl < minTransactionTTL)
    throw new IllegalArgumentException(s"The TTL must be greater or equal than $minTransactionTTL seconds.")

  /**
    * Save stream info in metadata
    */
  def save(): Unit = {
    val session = metadataStorage.getSession()
    Stream.changeStream(session, name, partitionsCount, ttl, description)
  }

}
