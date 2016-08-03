package com.bwsw.tstreams.agents.producer


/**
  * Available insert types for producer
  */
object DataInsertType {

  /**
    * Basic trait for insertion type
    */
  trait InsertType

  /**
    * With this statement elements will be sent
    * every producer.send(obj : T) invoke
    */
  case object SingleElementInsert extends InsertType

  /**
    * With this statement elements will be sent only after
    * the local batch with specified size will be filled
    *
    * @param batchSize Size of batch to put in storage
    */
  case class BatchInsert(batchSize: Int) extends InsertType

}