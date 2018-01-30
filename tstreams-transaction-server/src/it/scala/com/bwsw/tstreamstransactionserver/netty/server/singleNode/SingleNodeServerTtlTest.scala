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

package com.bwsw.tstreamstransactionserver.netty.server.singleNode

import java.nio.file.Paths

import com.bwsw.tstreamstransactionserver.netty.client.ClientBuilder
import com.bwsw.tstreamstransactionserver.netty.server.db.rocks.RocksDbConnection
import com.bwsw.tstreamstransactionserver.netty.server.storage.Storage
import com.bwsw.tstreamstransactionserver.netty.server.storage.rocks.MultiAndSingleNodeRockStorage
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates.Checkpointed
import com.bwsw.tstreamstransactionserver.rpc.{ProducerTransaction, TransactionInfo}
import com.bwsw.tstreamstransactionserver.util.Utils.startZookeeperServer
import com.bwsw.tstreamstransactionserver.util.{Utils, ZkSeverTxnServerTxnClient}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random

/** Tests that expired transactions deletes from RocksDB storage
  *
  * @author Pavel Tomskikh
  */
class SingleNodeServerTtlTest extends FlatSpec with Matchers with BeforeAndAfterAll {

  private val compactionInterval = 1
  private val ttl = 10
  private val secondsWait = 5
  private val serverBuilder = new SingleNodeServerBuilder()

  private val clientBuilder = new ClientBuilder()
  private val (zkServer, zkClient) = startZookeeperServer


  "SingleNodeServer" should "remove expired transactions" in {
    val rocksStorageOptions = serverBuilder.getRocksStorageOptions.copy(
      transactionExpungeDelaySec = ttl)
    val storageOptions = serverBuilder.getStorageOptions.copy(
      dataCompactionInterval = compactionInterval)
    val bundle = Utils.startTransactionServerAndClient(
      zkClient,
      serverBuilder.withServerRocksStorageOptions(rocksStorageOptions).withServerStorageOptions(storageOptions),
      clientBuilder)

    bundle.operate { _ =>
      val client = bundle.client
      val partition = 0
      val dataAmount = 10
      val data = Seq.fill(dataAmount)(Array.fill(10)(Random.nextInt().toByte))
      val streamID = Await.result(client.putStream("test_stream", 1, None, ttl), secondsWait.seconds)

      val transactionID = Await.result(
        client.putSimpleTransactionAndData(streamID, partition, data),
        secondsWait.seconds)

      Thread.sleep(compactionInterval * 1000)

      Await.result(
        client.getTransaction(streamID, partition, transactionID),
        secondsWait.seconds) should matchPattern {
        case TransactionInfo(true, Some(ProducerTransaction(`streamID`, `partition`, `transactionID`, Checkpointed, `dataAmount`, _))) =>
      }
      Await.result(
        client.getTransactionData(streamID, partition, transactionID, 0, dataAmount),
        secondsWait.seconds) should contain theSameElementsInOrderAs data


      transactionExistsInStorage(bundle) shouldBe true
      transactionDataExistsInStorage(bundle, streamID) shouldBe true


      Thread.sleep((ttl + secondsWait) * 1000)

      Await.result(
        client.getTransaction(streamID, partition, transactionID),
        secondsWait.seconds) should matchPattern { case TransactionInfo(_, None) => }
      Await.result(
        client.getTransactionData(streamID, partition, transactionID, 0, dataAmount),
        secondsWait.seconds) shouldBe empty

      transactionExistsInStorage(bundle) shouldBe false
      transactionDataExistsInStorage(bundle, streamID) shouldBe false
    }
  }


  override def afterAll(): Unit = {
    zkClient.close()
    zkServer.close()
  }


  private def transactionExistsInStorage(bundle: ZkSeverTxnServerTxnClient) = {
    val rocksStorage = new MultiAndSingleNodeRockStorage(
      bundle.serverBuilder.getStorageOptions,
      bundle.serverBuilder.getRocksStorageOptions,
      readOnly = true)

    val metadataDB = rocksStorage.getStorageManager.getDatabase(Storage.TRANSACTION_ALL_STORE)
    val iterator = metadataDB.iterator
    iterator.seekToFirst()
    val result = iterator.isValid

    rocksStorage.getStorageManager.closeDatabases()

    result
  }

  private def transactionDataExistsInStorage(bundle: ZkSeverTxnServerTxnClient, streamID: Int) = {
    val storageOptions = bundle.serverBuilder.getStorageOptions
    val path = Paths.get(storageOptions.path, storageOptions.dataDirectory, streamID.toString).toString
    val connection = new RocksDbConnection(
      rocksStorageOpts = bundle.serverBuilder.getRocksStorageOptions,
      path,
      storageOptions.dataCompactionInterval,
      readOnly = true)

    val iterator = connection.iterator
    iterator.seekToFirst()
    val result = iterator.isValid

    connection.close()

    result
  }
}
