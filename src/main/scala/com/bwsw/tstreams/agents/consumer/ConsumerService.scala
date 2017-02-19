package com.bwsw.tstreams.agents.consumer

import com.bwsw.tstreams.common.StorageClient

import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.duration._


/**
  * Consumer entity for interact with consumers metadata
  *
  * @param storageClient Session with metadata
  */
class ConsumerService(storageClient: StorageClient) {



  /**
    * Checking exist or not concrete consumer
    *
    * @param consumerName Name of the consumer
    * @return Exist or not concrete consumer
    */
  def offsetExists(consumerName: String, stream: String, partition: Int): Boolean = {
    Await.result(storageClient.client.getConsumerState((consumerName, stream, partition)), 1.minute) > 0
  }

  /**
    * Saving offset batch
    *
    * @param consumerName                        Name of the consumer
    * @param stream                      Name of the specific stream
    * @param partitionAndLastTransaction Set of partition and last transaction pairs to save
    */
  def saveBatchOffset(consumerName: String, stream: String, partitionAndLastTransaction: scala.collection.mutable.Map[Int, Long]): Unit = {
    val batch = ListBuffer[transactionService.rpc.ConsumerTransaction]()
    batch.appendAll(partitionAndLastTransaction.map { case (partition, offset) =>
      new RPCConsumerTransaction(consumerName, stream, partition, offset)
    })

    Await.result(storageClient.client.putTransactions(Nil, batch), 1.minute)
  }

  /**
    * Saving single offset
    *
    * @param consumerName      Name of the specific consumer
    * @param stream    Name of the specific stream
    * @param partition Name of the specific partition
    * @param offset    Offset to save
    */
  def saveSingleOffset(consumerName: String, stream: String, partition: Int, offset: Long): Unit = {
    Await.result(storageClient.client.putTransaction(new RPCConsumerTransaction(consumerName, stream, partition, offset)), 1.minute)
  }

  /**
    * Retrieving specific offset for particular consumer
    *
    * @param consumerName      Name of the specific consumer
    * @param stream    Name of the specific stream
    * @param partition Name of the specific partition
    * @return Offset
    */
  def getLastSavedOffset(consumerName: String, stream: String, partition: Int): Long = {
    Await.result(storageClient.client.getConsumerState((consumerName, stream, partition)), 1.minute)
  }
}
