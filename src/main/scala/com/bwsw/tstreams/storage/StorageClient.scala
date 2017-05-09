package com.bwsw.tstreams.storage

import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreams.agents.consumer.RPCConsumerTransaction
import com.bwsw.tstreams.streams.Stream
import com.bwsw.tstreamstransactionserver.options.ClientBuilder
import com.bwsw.tstreamstransactionserver.options.ClientOptions.{AuthOptions, ConnectionOptions}
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.rpc.{CommitLogInfo, ConsumerTransaction, ProducerTransaction, TransactionStates}
import org.apache.zookeeper.KeeperException.BadArgumentsException
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


object StorageClient {
  private val logger = LoggerFactory.getLogger(this.getClass)
  var maxAwaiTimeout = 1.minute
}

/**
  *
  * @param clientOptions
  * @param authOptions
  * @param zookeeperOptions
  */
class StorageClient(clientOptions: ConnectionOptions, authOptions: AuthOptions, zookeeperOptions: ZookeeperOptions) {
  private val map = scala.collection.mutable.Map[String, String]()
  private val clientBuilder = new ClientBuilder()

  private val client = clientBuilder.withConnectionOptions(clientOptions).withAuthOptions(authOptions).withZookeeperOptions(zookeeperOptions).build()

  val isShutdown = new AtomicBoolean(false)

  def shutdown() = {
    isShutdown.set(true)
    client.shutdown()
  }

  /**
    * Getting existing stream
    *
    * @param streamName Name of the stream
    * @return Stream instance
    */
  def loadStream(streamName: String, timeout: Duration = StorageClient.maxAwaiTimeout): Stream = {
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
  def createStream(streamName: String, partitionsCount: Int, ttl: Long, description: String,
                   timeout: Duration = StorageClient.maxAwaiTimeout): Stream = {
    if (!Await.result(client.putStream(streamName, partitionsCount, Some(description), ttl), timeout))
      throw new IllegalArgumentException(s"Stream $streamName already exists.")

    new Stream(this, streamName, partitionsCount, ttl, description)
  }


  /**
    * Deleting concrete stream
    *
    * @param streamName Name of the stream to delete
    */
  def deleteStream(streamName: String, timeout: Duration = StorageClient.maxAwaiTimeout) = {
    Await.result(client.delStream(streamName), timeout)
  }


  /**
    * Checking exist concrete stream or not
    *
    * @param streamName Name of the stream to check
    */
  def checkStreamExists(streamName: String, timeout: Duration = StorageClient.maxAwaiTimeout) = {
    Await.result(client.checkStreamExists(streamName), timeout)
  }


  /**
    * Checking exist or not concrete consumer
    *
    * @param consumerName Name of the consumer
    * @return Exist or not concrete consumer
    */
  def checkConsumerOffsetExists(consumerName: String, stream: String, partition: Int,
                                timeout: Duration = StorageClient.maxAwaiTimeout) = {
    Await.result(client.getConsumerState(name = consumerName, stream = stream, partition = partition), timeout) > 0
  }

  /**
    * Saving offset batch
    *
    * @param consumerName                Name of the consumer
    * @param stream                      Name of the specific stream
    * @param partitionAndLastTransaction Set of partition and last transaction pairs to save
    */
  def saveConsumerOffsetBatch(consumerName: String, stream: String, partitionAndLastTransaction: scala.collection.mutable.Map[Int, Long],
                              timeout: Duration = StorageClient.maxAwaiTimeout) = {
    val batch = ListBuffer[ConsumerTransaction]()
    batch.appendAll(partitionAndLastTransaction.map { case (partition, offset) =>
      new RPCConsumerTransaction(consumerName, stream, partition, offset)
    })

    Await.result(client.putTransactions(Nil, batch), timeout)
  }

  /**
    * Saving single offset
    *
    * @param consumerName Name of the specific consumer
    * @param stream       Name of the specific stream
    * @param partition    Name of the specific partition
    * @param offset       Offset to save
    */
  def saveConsumerOffset(consumerName: String, stream: String, partition: Int, offset: Long,
                         timeout: Duration = StorageClient.maxAwaiTimeout): Unit = {
    Await.result(client.putTransaction(new RPCConsumerTransaction(consumerName, stream, partition, offset)), timeout)
  }

  /**
    * Retrieving specific offset for particular consumer
    *
    * @param consumerName Name of the specific consumer
    * @param stream       Name of the specific stream
    * @param partition    Name of the specific partition
    * @return Offset
    */
  def getLastSavedConsumerOffset(consumerName: String, stream: String, partition: Int,
                                 timeout: Duration = StorageClient.maxAwaiTimeout): Long = {
    Await.result(client.getConsumerState(name = consumerName, stream = stream, partition = partition), timeout)
  }

  def putTransactionSync(transaction: ProducerTransaction, timeout: Duration = StorageClient.maxAwaiTimeout) = {
    val f = client.putProducerState(transaction)
    if(StorageClient.logger.isDebugEnabled)
      StorageClient.logger.debug(s"Placed $transaction [putTransactionSync]")
    Await.result(f, timeout)
  }

  def putTransactionWithDataSync[T](transaction: ProducerTransaction, data: ListBuffer[Array[Byte]], lastOffset: Int,
                                    timeout: Duration = StorageClient.maxAwaiTimeout) = {
    val f = client.putProducerStateWithData(transaction, data, lastOffset)
    if(StorageClient.logger.isDebugEnabled)
      StorageClient.logger.debug(s"Placed $transaction [putTransactionWithDataSync]")
    Await.result(f, timeout)
  }

  def putInstantTransactionSync(stream: String, partition: Int, transactionID: Long, data: Seq[Array[Byte]],
                                timeout: Duration = StorageClient.maxAwaiTimeout): Unit = {
    val f = client.putSimpleTransactionAndData(stream, partition, transactionID, data)
    Await.result(f, timeout)
  }

  def putInstantTransactionUnreliable(stream: String, partition: Int, transactionID: Long, data: Seq[Array[Byte]]): Unit = {
    client.putSimpleTransactionAndDataWithoutResponse(stream, partition, transactionID, data)
  }

  def getTransaction(streamName: String, partition: Integer, transactionID: Long,
                     timeout: Duration = StorageClient.maxAwaiTimeout): Option[ProducerTransaction] = {
    while (true) {
      val txnInfo = Await.result(client.getTransaction(streamName, partition, transactionID), timeout)
      (txnInfo.exists, txnInfo.transaction) match {
        case (true, t: Option[ProducerTransaction]) => return t
        case (false, _) =>
        case _ => throw new BadArgumentsException(s"Expected to get (Boolean, Option[ProducerTransaction]).")
      }
    }
    None
  }

  def scanTransactions(streamName: String, partition: Integer, from: Long, to: Long, count: Int, states: Set[TransactionStates],
                       timeout: Duration = StorageClient.maxAwaiTimeout): (Long, Seq[ProducerTransaction]) = {
    val txnInfo = Await.result(client.scanTransactions(streamName, partition, from, to, count, states), timeout)
    (txnInfo.lastOpenedTransactionID, txnInfo.producerTransactions)
  }

  def getLastTransactionId(streamName: String, partition: Integer, timeout: Duration = StorageClient.maxAwaiTimeout): Long = {
    Await.result(client.getLastCheckpointedTransaction(streamName, partition), timeout)
  }

  def getCommitLogOffsets(timeout: Duration = StorageClient.maxAwaiTimeout): CommitLogInfo = {
    Await.result(client.getCommitLogOffsets(), timeout)
  }

  def getTransactionData(stream: String, partition: Int, transaction: Long, from: Int, to: Int,
                         timeout: Duration = StorageClient.maxAwaiTimeout): Seq[Array[Byte]] = {
    Await.result(client.getTransactionData(stream, partition, transaction, from, to), timeout)
  }

  def putTransactions(producerTransactions: Seq[com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction],
                      consumerTransactions: Seq[com.bwsw.tstreamstransactionserver.rpc.ConsumerTransaction],
                      timeout: Duration = StorageClient.maxAwaiTimeout): Boolean = {
    Await.result(client.putTransactions(producerTransactions, consumerTransactions), timeout)
  }

  def putTransactionData(stream: String, partition: Int, transaction: Long, data: Seq[Array[Byte]], from: Int): Future[Boolean] = {
    client.putTransactionData(stream, partition, transaction, data, from)
  }

}
