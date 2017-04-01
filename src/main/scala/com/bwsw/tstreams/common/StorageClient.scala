package com.bwsw.tstreams.common

import com.bwsw.tstreams.agents.consumer.RPCConsumerTransaction
import com.bwsw.tstreams.streams.Stream
import com.bwsw.tstreamstransactionserver.options.ClientBuilder
import com.bwsw.tstreamstransactionserver.options.ClientOptions.{AuthOptions, ConnectionOptions}
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.rpc.{ConsumerTransaction, ProducerTransaction}
import org.apache.zookeeper.KeeperException.BadArgumentsException
import org.slf4j.LoggerFactory

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
class StorageClient(clientOptions: ConnectionOptions, authOptions: AuthOptions, zookeeperOptions: ZookeeperOptions) {
  private val map = scala.collection.mutable.Map[String, String]()
  private val clientBuilder = new ClientBuilder()

  val client = clientBuilder.withConnectionOptions(clientOptions).withAuthOptions(authOptions).withZookeeperOptions(zookeeperOptions).build()

  /**
    * Getting existing stream
    *
    * @param streamName Name of the stream
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
    if (Await.result(client.putStream(streamName, partitionsCount, Some(description), ttl), timeout) == false)
      throw new IllegalArgumentException(s"Stream ${streamName} already exists.")

    new Stream(this, streamName, partitionsCount, ttl, description)
  }


  /**
    * Deleting concrete stream
    *
    * @param streamName Name of the stream to delete
    */
  def deleteStream(streamName: String, timeout: Duration = 1.minute) = Await.result(client.delStream(streamName), timeout)


  /**
    * Checking exist concrete stream or not
    *
    * @param streamName Name of the stream to check
    */
  def checkStreamExists(streamName: String, timeout: Duration = 1.minute) = Await.result(client.checkStreamExists(streamName), timeout)


  /**
    * Checking exist or not concrete consumer
    *
    * @param consumerName Name of the consumer
    * @return Exist or not concrete consumer
    */
  def checkConsumerOffsetExists(consumerName: String, stream: String, partition: Int, timeout: Duration = 1.minute) = {
    Await.result(client.getConsumerState(name = consumerName, stream = stream, partition = partition), timeout) > 0
  }

  /**
    * Saving offset batch
    *
    * @param consumerName                Name of the consumer
    * @param stream                      Name of the specific stream
    * @param partitionAndLastTransaction Set of partition and last transaction pairs to save
    */
  def saveConsumerOffsetBatch(consumerName: String, stream: String, partitionAndLastTransaction: scala.collection.mutable.Map[Int, Long], timeout: Duration = 1.minute) = {
    val batch = ListBuffer[ConsumerTransaction]()
    batch.appendAll(partitionAndLastTransaction.map { case (partition, offset) => {
      val t = new RPCConsumerTransaction(consumerName, stream, partition, offset)
      println(s"Consumer Batch Checkpoint Add: ${t}")
      t
    }
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
  def saveConsumerOffset(consumerName: String, stream: String, partition: Int, offset: Long, timeout: Duration = 1.minute): Unit = {
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
  def getLastSavedConsumerOffset(consumerName: String, stream: String, partition: Int, timeout: Duration = 1.minute): Long = {
    Await.result(client.getConsumerState(name = consumerName, stream = stream, partition = partition), timeout)
  }

  def putTransaction[T](transaction: ProducerTransaction, async: Boolean, timeout: Duration = 1.minute)(onComplete: ProducerTransaction => T) = {

    val f = client.putProducerState(transaction)
    if (async) {
      import ExecutionContext.Implicits.global

      f onComplete {
        case Success(res) => onComplete(transaction)
        case Failure(reason) => throw reason
      }
    } else {
      Await.result(f, timeout)
      onComplete(transaction)
    }
  }

  def getTransaction(streamName: String, partition: Integer, transactionID: Long, timeout: Duration = 1.minute): Option[ProducerTransaction] = {
    while(true) {
      val txnInfo = Await.result(client.getTransaction(streamName, partition, transactionID), timeout)
      (txnInfo.exists, txnInfo.transaction) match {
        case (true, t: Option[ProducerTransaction]) => return t
        case (false, _) =>
        case _ => throw new BadArgumentsException(s"Expected to get (Boolean, Option[ProducerTransaction]).")
      }
    }
    return None
  }

  def scanTransactions(streamName: String, partition: Integer, from: Long, to: Long, lambda: ProducerTransaction => Boolean = txn => true, timeout: Duration = 1.minute): (Long, Seq[ProducerTransaction]) = {
    val txnInfo = Await.result(client.scanTransactions(streamName, partition, from, to, lambda), timeout)
    (txnInfo.lastOpenedTransactionID, txnInfo.producerTransactions)
  }

  def getLastTransactionId(streamName: String, partition: Integer, timeout: Duration = 1.minute): Long = {
    Await.result(client.getLastCheckpointedTransaction(streamName, partition), timeout)
  }
}
