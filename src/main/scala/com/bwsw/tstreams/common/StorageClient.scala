package com.bwsw.tstreams.common

import com.bwsw.tstreams.agents.consumer.RPCConsumerTransaction
import com.bwsw.tstreams.streams.Stream
import com.bwsw.tstreamstransactionserver.options.{AuthOptions, ClientBuilder, ClientOptions, ZookeeperOptions}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.concurrent.Await
import scala.concurrent.duration._


object StorageClient {
  private val logger = LoggerFactory.getLogger(this.getClass)
}
/**
  *
  * @param clientOptions
  * @param authOptions
  * @param zookeeperOptions
  */
class StorageClient(clientOptions: ClientOptions, authOptions: AuthOptions, zookeeperOptions: ZookeeperOptions) {
  private val map = scala.collection.mutable.Map[String,String]()
  private val clientBuilder = new ClientBuilder()

  val client = clientBuilder.withClientOptions(clientOptions).withAuthOptions(authOptions).withZookeeperOptions(zookeeperOptions).build()

  /**
    * Getting existing stream
    *
    * @param streamName      Name of the stream
    * @return Stream instance
    */
  def loadStream(streamName: String, timeout: Duration = 1.minute): Stream = {
    val rpcStream = Await.result(client.getStream(streamName), timeout)

    new Stream(client = this, name = rpcStream.name, partitionsCount = rpcStream.partitions,
      ttl = rpcStream.ttl, description = rpcStream.description.fold("")(x => x))
  }

  /**
    * Creating stream
    *
    * @param streamName      Name of the stream
    * @param partitionsCount Number of stream partitions
    * @param description     Some additional info about stream
    * @param ttl             Expiration time of single transaction in seconds
    */
  def createStream(streamName: String, partitionsCount: Int, ttl: Long, description: String, timeout: Duration = 1.minute): Stream = {
    //TODO: fixit Long -> Int (TTL)
    if (Await.result(client.putStream(streamName, partitionsCount, Some(description), ttl.toInt), timeout) == false)
      throw new IllegalArgumentException(s"Stream ${streamName} already exists.")

    new Stream(this, streamName, partitionsCount, ttl, description)
  }


  /**
    * Deleting concrete stream
    *
    * @param streamName      Name of the stream to delete
    */
  def deleteStream(streamName: String, timeout: Duration = 1.minute) = Await.result(client.delStream(streamName), timeout)




  /**
    * Checking exist concrete stream or not
    *
    * @param streamName      Name of the stream to check
    */
  def checkStreamExists(streamName: String, timeout: Duration = 1.minute) = Await.result(client.doesStreamExist(streamName), timeout)


  /**
    * Checking exist or not concrete consumer
    *
    * @param consumerName Name of the consumer
    * @return Exist or not concrete consumer
    */
  def checkConsumerOffsetExists(consumerName: String, stream: String, partition: Int, timeout: Duration = 1.minute) = {
    Await.result(client.getConsumerState((consumerName, stream, partition)), timeout) > 0
  }

  /**
    * Saving offset batch
    *
    * @param consumerName                        Name of the consumer
    * @param stream                      Name of the specific stream
    * @param partitionAndLastTransaction Set of partition and last transaction pairs to save
    */
  def saveConsumerOffsetBatch(consumerName: String, stream: String, partitionAndLastTransaction: scala.collection.mutable.Map[Int, Long], timeout: Duration = 1.minute) = {
    val batch = ListBuffer[transactionService.rpc.ConsumerTransaction]()
    batch.appendAll(partitionAndLastTransaction.map { case (partition, offset) =>
      new RPCConsumerTransaction(consumerName, stream, partition, offset)
    })

    Await.result(client.putTransactions(Nil, batch), timeout)
  }

  /**
    * Saving single offset
    *
    * @param consumerName      Name of the specific consumer
    * @param stream    Name of the specific stream
    * @param partition Name of the specific partition
    * @param offset    Offset to save
    */
  def saveConsumerOffset(consumerName: String, stream: String, partition: Int, offset: Long, timeout: Duration = 1.minute): Unit = {
    Await.result(client.putTransaction(new RPCConsumerTransaction(consumerName, stream, partition, offset)), timeout)
  }

  /**
    * Retrieving specific offset for particular consumer
    *
    * @param consumerName      Name of the specific consumer
    * @param stream    Name of the specific stream
    * @param partition Name of the specific partition
    * @return Offset
    */
  def getLastSavedConsumerOffset(consumerName: String, stream: String, partition: Int, timeout: Duration = 1.minute): Long = {
    Await.result(client.getConsumerState((consumerName, stream, partition)), timeout)
  }
}
