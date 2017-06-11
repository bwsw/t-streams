package com.bwsw.tstreams.common

import com.bwsw.tstreams.storage.StorageClient

class TransactionGenerator(storageClient: StorageClient) {
  def generateTransactionID = storageClient.generateTransaction()
  def generateTransactionIDForTimestamp(timestamp: Long) = storageClient.generateTransactionForTimestamp(timestamp)
}