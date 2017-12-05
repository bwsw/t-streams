/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.bwsw.tstreams.storage

import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreams.agents.consumer.RPCConsumerTransaction
import com.bwsw.tstreams.streams.Stream
import com.bwsw.tstreamstransactionserver.netty.client.ClientBuilder
import com.bwsw.tstreamstransactionserver.options.ClientOptions.{AuthOptions, ConnectionOptions}
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.rpc.{CommitLogInfo, ConsumerTransaction, ProducerTransaction, TransactionStates}
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.KeeperException.BadArgumentsException
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}


object StorageClient {
  private val logger = LoggerFactory.getLogger(this.getClass)
  var maxAwaiTimeout = 1.minute
}

class StorageClient(clientOptions: ConnectionOptions,
                    authOptions: AuthOptions,
                    zookeeperOptions: ZookeeperOptions,
                    curator: CuratorFramework) {
  private val clientBuilder = new ClientBuilder()

  private val client = clientBuilder
    .withConnectionOptions(clientOptions)
    .withAuthOptions(authOptions)
    .withZookeeperOptions(zookeeperOptions)
    .withCuratorConnection(curatorClient)
    .build()

  val isShutdown = new AtomicBoolean(false)

  def curatorClient: CuratorFramework = curator

  def authenticationKey: String = authOptions.key

  def shutdown(): Unit = {
    if (!isShutdown.getAndSet(true)) {
      client.shutdown()
      curator.close()
    }
  }

  /**
    * Getting existing stream
    *
    * @param streamName Name of the stream
    * @return Stream instance
    */
  def loadStream(streamName: String, timeout: Duration = StorageClient.maxAwaiTimeout): Stream = {
    val rpcStream = Await.result(client.getStream(streamName), timeout).get

    new Stream(curator = curator, client = this, id = rpcStream.id, name = rpcStream.name, partitionsCount = rpcStream.partitions,
      ttl = rpcStream.ttl, description = rpcStream.description.fold("")(x => x), path = rpcStream.zkPath)
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

    val streamID = Await.result(client.putStream(streamName, partitionsCount, Some(description), ttl), timeout)
    if (0 > streamID)
      throw new IllegalArgumentException(s"Stream $streamName already exists.")

    StorageClient.logger.warn(s"Created stream '$streamName' with $partitionsCount partitions and TTL: $ttl seconds.")

    loadStream(streamName)
  }


  /**
    * Deleting concrete stream
    *
    * @param streamName Name of the stream to delete
    */
  def deleteStream(streamName: String, timeout: Duration = StorageClient.maxAwaiTimeout): Boolean = {
    Await.result(client.delStream(streamName), timeout)
  }


  /**
    * Checking exist concrete stream or not
    *
    * @param streamName Name of the stream to check
    */
  def checkStreamExists(streamName: String, timeout: Duration = StorageClient.maxAwaiTimeout): Boolean = {
    Await.result(client.checkStreamExists(streamName), timeout)
  }


  /**
    * Checking exist or not concrete consumer
    *
    * @param consumerName Name of the consumer
    * @return Exist or not concrete consumer
    */
  def checkConsumerOffsetExists(consumerName: String, streamID: Int, partition: Int,
                                timeout: Duration = StorageClient.maxAwaiTimeout): Boolean = {
    Await.result(client.getConsumerState(name = consumerName, streamID = streamID, partition = partition), timeout) > 0
  }

  /**
    * Saving offset batch
    *
    * @param consumerName                Name of the consumer
    * @param streamID                    Name of the specific stream
    * @param partitionAndLastTransaction Set of partition and last transaction pairs to save
    */
  def saveConsumerOffsetBatch(consumerName: String, streamID: Int, partitionAndLastTransaction: scala.collection.mutable.Map[Int, Long],
                              timeout: Duration = StorageClient.maxAwaiTimeout): Boolean = {
    val batch = ListBuffer[ConsumerTransaction]()
    batch.appendAll(partitionAndLastTransaction.map { case (partition, offset) =>
      new RPCConsumerTransaction(consumerName, streamID, partition, offset)
    })

    Await.result(client.putTransactions(Nil, batch), timeout)
  }

  /**
    * Saving single offset
    *
    * @param consumerName Name of the specific consumer
    * @param streamID     Name of the specific stream
    * @param partition    Name of the specific partition
    * @param offset       Offset to save
    */
  def saveConsumerOffset(consumerName: String, streamID: Int, partition: Int, offset: Long,
                         timeout: Duration = StorageClient.maxAwaiTimeout): Unit = {
    Await.result(client.putTransaction(new RPCConsumerTransaction(consumerName, streamID, partition, offset)), timeout)
  }

  /**
    * Retrieving specific offset for particular consumer
    *
    * @param consumerName Name of the specific consumer
    * @param streamID     Name of the specific stream
    * @param partition    Name of the specific partition
    * @return Offset
    */
  def getLastSavedConsumerOffset(consumerName: String, streamID: Int, partition: Int,
                                 timeout: Duration = StorageClient.maxAwaiTimeout): Long = {
    Await.result(client.getConsumerState(name = consumerName, streamID = streamID, partition = partition), timeout)
  }

  def putTransactionSync(transaction: ProducerTransaction, timeout: Duration = StorageClient.maxAwaiTimeout): Boolean = {
    val f = client.putProducerState(transaction)
    if (StorageClient.logger.isDebugEnabled)
      StorageClient.logger.debug(s"Placed $transaction [putTransactionSync]")
    Await.result(f, timeout)
  }

  def putTransactionWithDataSync[T](transaction: ProducerTransaction, data: ListBuffer[Array[Byte]], lastOffset: Int,
                                    timeout: Duration = StorageClient.maxAwaiTimeout): Boolean = {
    val f = client.putProducerStateWithData(transaction, data, lastOffset)
    if (StorageClient.logger.isDebugEnabled)
      StorageClient.logger.debug(s"Placed $transaction [putTransactionWithDataSync]")
    Await.result(f, timeout)
  }

  def openTransactionSync(streamID: Int, partition: Int, transactionTtlMs: Long, timeout: Duration = StorageClient.maxAwaiTimeout): Long = {
    val f = client.openTransaction(streamID, partition, transactionTtlMs)
    Await.result(f, timeout)
  }

  def putInstantTransactionSync(streamID: Int, partition: Int, data: Seq[Array[Byte]],
                                timeout: Duration = StorageClient.maxAwaiTimeout): Long = {
    val f = client.putSimpleTransactionAndData(streamID, partition, data)
    Await.result(f, timeout)
  }

  def putInstantTransactionUnreliable(streamID: Int, partition: Int, data: Seq[Array[Byte]]): Unit = {
    client.putSimpleTransactionAndDataWithoutResponse(streamID, partition, data)
  }

  def getTransaction(streamID: Int, partition: Integer, transactionID: Long,
                     timeout: Duration = StorageClient.maxAwaiTimeout): Option[ProducerTransaction] = {
    while (true) {
      val txnInfo = Await.result(client.getTransaction(streamID, partition, transactionID), timeout)
      (txnInfo.exists, txnInfo.transaction) match {
        case (true, t: Option[ProducerTransaction]) => return t
        case (false, _) =>
        case _ => throw new BadArgumentsException(s"Expected to get (Boolean, Option[ProducerTransaction]).")
      }
    }
    None
  }

  def scanTransactions(streamID: Int, partition: Integer, from: Long, to: Long, count: Int, states: Set[TransactionStates],
                       timeout: Duration = StorageClient.maxAwaiTimeout): (Long, Seq[ProducerTransaction]) = {
    val txnInfo = Await.result(client.scanTransactions(streamID, partition, from, to, count, states), timeout)
    (txnInfo.lastOpenedTransactionID, txnInfo.producerTransactions)
  }

  def getLastTransactionId(streamID: Int, partition: Integer, timeout: Duration = StorageClient.maxAwaiTimeout): Long = {
    Await.result(client.getLastCheckpointedTransaction(streamID, partition), timeout)
  }

  def getCommitLogOffsets(timeout: Duration = StorageClient.maxAwaiTimeout): CommitLogInfo = {
    Await.result(client.getCommitLogOffsets(), timeout)
  }

  def getTransactionData(streamID: Int, partition: Int, transaction: Long, from: Int, to: Int,
                         timeout: Duration = StorageClient.maxAwaiTimeout): Seq[Array[Byte]] = {
    Await.result(client.getTransactionData(streamID, partition, transaction, from, to), timeout)
  }

  def putTransactions(producerTransactions: Seq[com.bwsw.tstreamstransactionserver.rpc.ProducerTransaction],
                      consumerTransactions: Seq[com.bwsw.tstreamstransactionserver.rpc.ConsumerTransaction],
                      timeout: Duration = StorageClient.maxAwaiTimeout): Boolean = {
    val res = Await.result(client.putTransactions(producerTransactions, consumerTransactions), timeout)
    if (StorageClient.logger.isDebugEnabled) {
      StorageClient.logger.debug(s"Placed ProducerTransactions $producerTransactions  [putTransactions]")
      StorageClient.logger.debug(s"Placed ConsumerStates $consumerTransactions  [putTransactions]")
    }
    res
  }

  def putTransactionData(streamID: Int, partition: Int, transaction: Long, data: Seq[Array[Byte]], from: Int): Future[Boolean] = {
    client.putTransactionData(streamID, partition, transaction, data, from)
  }

  def generateTransaction(timeout: Duration = StorageClient.maxAwaiTimeout): Long = {
    Await.result(client.getTransaction(), timeout)
  }

  def generateTransactionForTimestamp(time: Long, timeout: Duration = StorageClient.maxAwaiTimeout): Long = {
    Await.result(client.getTransaction(time), timeout)
  }

}
