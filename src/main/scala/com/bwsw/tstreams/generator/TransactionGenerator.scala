package com.bwsw.tstreams.generator

import com.bwsw.tstreams.storage.StorageClient

/**
  * Entity for generating new transaction time
  */
class TransactionGenerator(storageClient: StorageClient) {

  /**
    * @return Transaction ID
    */
  def getTransaction(): Long = storageClient.generateTransaction()

  /**
    * @return time based on transaction
    */
  def getTransaction(timestamp: Long) = storageClient.generateTransactionForTimestamp(timestamp)

}