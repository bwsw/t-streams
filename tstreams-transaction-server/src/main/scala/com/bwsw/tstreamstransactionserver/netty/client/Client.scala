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
package com.bwsw.tstreamstransactionserver.netty.client

import com.bwsw.tstreamstransactionserver.netty.client.api.TTSInetClient
import com.bwsw.tstreamstransactionserver.options.ClientOptions.{AuthOptions, ConnectionOptions}
import com.bwsw.tstreamstransactionserver.options.CommonOptions.{TracingOptions, ZookeeperOptions}
import com.bwsw.tstreamstransactionserver.rpc
import com.bwsw.tstreamstransactionserver.rpc._
import org.apache.curator.framework.CuratorFramework

import scala.concurrent.Future

/** A client who connects to a server.
  *
  *
  */
class Client(clientOpts: ConnectionOptions,
             authOpts: AuthOptions,
             zookeeperOptions: ZookeeperOptions,
             externalCuratorClient: Option[CuratorFramework] = None,
             tracingOptions: TracingOptions = TracingOptions())
  extends TTSInetClient {


  private val inetClientProxy = new InetClientProxy(
    clientOpts,
    authOpts,
    zookeeperOptions,
    onZKConnectionStateChanged,
    onServerConnectionLost(),
    onRequestTimeout(),
    externalCuratorClient,
    tracingOptions
  )


  override def getCommitLogOffsets(): Future[CommitLogInfo] =
    inetClientProxy.getCommitLogOffsets()

  override def putProducerStateWithData(producerTransaction: ProducerTransaction,
                                        data: Seq[Array[Byte]],
                                        from: Int): Future[Boolean] =
    inetClientProxy.putProducerStateWithData(
      producerTransaction,
      data,
      from
    )

  override def putSimpleTransactionAndData(streamID: Int,
                                           partition: Int,
                                           data: Seq[Array[Byte]]): Future[Long] =
    inetClientProxy.putSimpleTransactionAndData(
      streamID,
      partition,
      data
    )

  override def putSimpleTransactionAndDataWithoutResponse(streamID: Int,
                                                          partition: Int,
                                                          data: Seq[Array[Byte]]): Unit =
    inetClientProxy.putSimpleTransactionAndDataWithoutResponse(
      streamID,
      partition,
      data
    )

  override def putTransactionData(streamID: Int,
                                  partition: Int,
                                  transaction: Long,
                                  data: Seq[Array[Byte]],
                                  from: Int): Future[Boolean] = {
    inetClientProxy.putTransactionData(
      streamID,
      partition,
      transaction,
      data,
      from
    )
  }

  override def getTransactionData(streamID: Int,
                                  partition: Int,
                                  transaction: Long,
                                  from: Int,
                                  to: Int): Future[Seq[Array[Byte]]] =
    inetClientProxy.getTransactionData(
      streamID,
      partition,
      transaction,
      from,
      to
    )

  override def putStream(stream: String,
                         partitions: Int,
                         description: Option[String],
                         ttl: Long): Future[Int] =
    inetClientProxy.putStream(
      stream,
      partitions,
      description,
      ttl
    )

  override def putStream(stream: StreamValue): Future[Int] =
    inetClientProxy.putStream(stream)

  override def delStream(name: String): Future[Boolean] =
    inetClientProxy.delStream(name)

  override def getStream(name: String): Future[Option[rpc.Stream]] =
    inetClientProxy.getStream(name)

  override def checkStreamExists(name: String): Future[Boolean] =
    inetClientProxy.checkStreamExists(name)

  override def putConsumerCheckpoint(consumerTransaction: ConsumerTransaction): Future[Boolean] =
    inetClientProxy.putConsumerCheckpoint(consumerTransaction)

  override def getConsumerState(name: String,
                                streamID: Int,
                                partition: Int): Future[Long] =
    inetClientProxy.getConsumerState(
      name,
      streamID,
      partition
    )

  override def getTransaction(): Future[Long] =
    inetClientProxy.getTransaction()

  override def getTransaction(timestamp: Long): Future[Long] =
    inetClientProxy.getTransaction(timestamp)

  override def putTransactions(producerTransactions: Seq[ProducerTransaction],
                               consumerTransactions: Seq[ConsumerTransaction]): Future[Boolean] =
    inetClientProxy.putTransactions(
      producerTransactions,
      consumerTransactions
    )

  override def putProducerState(transaction: ProducerTransaction): Future[Boolean] =
    inetClientProxy.putProducerState(
      transaction
    )

  override def putTransaction(transaction: ConsumerTransaction): Future[Boolean] =
    inetClientProxy.putTransaction(
      transaction
    )

  override def openTransaction(streamID: Int,
                               partitionID: Int,
                               transactionTTLMs: Long): Future[Long] =
    inetClientProxy.openTransaction(
      streamID,
      partitionID,
      transactionTTLMs
    )

  override def getTransaction(streamID: Int,
                              partition: Int,
                              transaction: Long): Future[TransactionInfo] =
    inetClientProxy.getTransaction(
      streamID,
      partition,
      transaction
    )


  override def getLastCheckpointedTransaction(streamID: Int,
                                              partition: Int): Future[Long] =
    inetClientProxy.getLastCheckpointedTransaction(
      streamID,
      partition
    )

  override def scanTransactions(streamID: Int,
                                partition: Int,
                                from: Long,
                                to: Long, count:
                                Int, states: Set[TransactionStates]): Future[ScanTransactionsInfo] =
    inetClientProxy.scanTransactions(
      streamID,
      partition,
      from,
      to,
      count,
      states
    )

  override def shutdown(): Unit =
    inetClientProxy.shutdown()
}