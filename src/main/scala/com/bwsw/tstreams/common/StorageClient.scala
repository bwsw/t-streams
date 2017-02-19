package com.bwsw.tstreams.common

import com.bwsw.tstreams.agents.consumer.RPCConsumerTransaction
import com.bwsw.tstreams.streams.{Stream, TransactionDatabase, TransactionRecord}
import com.bwsw.tstreamstransactionserver.options.{AuthOptions, ClientBuilder, ClientOptions, ZookeeperOptions}
import org.slf4j.LoggerFactory
import transactionService.rpc.{ProducerTransaction, TransactionStates}

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.util.{Failure, Success}


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

  def putTransaction[T](streamName: String, transaction: TransactionRecord, async: Boolean, timeout: Duration = 1.minute)(onComplete: ProducerTransaction => T) = {

    val tr = new ProducerTransaction {
      override def stream: String = streamName
      override def partition: Int = transaction.partition
      override def transactionID: Long = transaction.transactionID
      override def state: TransactionStates = transaction.state
      override def quantity: Int = transaction.count
      override def keepAliveTTL: Long = transaction.ttl
    }

    val f = client.putTransaction(tr)
    if (async) {
      import ExecutionContext.Implicits.global

      f onComplete {
        case Success(res) => onComplete(tr)
        case Failure(reason) => throw reason
      }
    } else {
      Await.result(f, timeout)
      onComplete(tr)
    }
  }

  def getTransaction(streamName: String, partition: Integer, transactionID: Long, timeout: Duration = 1.minute): Option[TransactionRecord] = {
    val txnSeq = Await.result(client.scanTransactions(streamName, partition, transactionID, transactionID), timeout)
    txnSeq.headOption.map(t => TransactionRecord(partition = partition, transactionID = transactionID, state = t.state, count = t.quantity, ttl = t.keepAliveTTL))
  }

  def scanTransactionsInterval(streamName: String, partition: Integer, interval: Long, timeout: Duration = 1.minute): Seq[TransactionRecord] = {
    val from = interval * TransactionDatabase.AGGREGATION_INTERVAL
    val to   = (interval+1) * TransactionDatabase.AGGREGATION_INTERVAL - 1
    StorageClient.logger.info(s"scanTransactionsInterval(${partition},${interval})")
    StorageClient.logger.info(s"client.scanTransactions(${streamName}, ${partition}, ${from},${to}")
    val txnSeq = Await.result(client.scanTransactions(streamName, partition, from, to), timeout)
    txnSeq.map(t => TransactionRecord(partition, t.transactionID, t.state, t.quantity, t.keepAliveTTL))
  }

}
