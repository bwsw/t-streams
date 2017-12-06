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

package com.bwsw.tstreams.benchmark

import com.bwsw.tstreams.agents.producer.NewProducerTransactionPolicy
import com.bwsw.tstreams.env.{ConfigurationOptions, TStreamsFactory}

/**
  * Provides method to measure time of [[com.bwsw.tstreams.agents.producer.Producer]]'s methods
  *
  * @param address    ZooKeeper address
  * @param token      authentication token
  * @param prefix     path to master node in ZooKeeper
  * @param stream     stream name. Stream will be created if it doesn't exists.
  * @param partitions amount of stream partitions
  * @author Pavel Tomskikh
  */
class ProducerBenchmark(address: String,
                        token: String,
                        prefix: String,
                        stream: String,
                        partitions: Int) {

  private val factory = new TStreamsFactory
  factory.setProperty(ConfigurationOptions.Coordination.endpoints, address)
  factory.setProperty(ConfigurationOptions.Common.authenticationKey, token)
  factory.setProperty(ConfigurationOptions.Coordination.path, prefix)

  private val streamTtl = 3600
  private val storageClient = factory.getStorageClient()
  if (!storageClient.checkStreamExists(stream))
    storageClient.createStream(stream, partitions, streamTtl, "benchmark")


  /**
    * Measures time of [[com.bwsw.tstreams.agents.producer.Producer]]'s methods
    *
    * @param iterations         amount of measurements
    * @param dataSize           size of data sent in each transaction
    * @param withCheckpoint     if true, a checkpoint will be performed for each transaction
    * @param progressReportRate progress will be printed to the console after this number of iterations
    * @return measurement result
    */
  def run(iterations: Int,
          dataSize: Int = 100,
          withCheckpoint: Boolean = true,
          progressReportRate: Int = 1000): ProducerBenchmark.Result = {
    val producer = factory.getProducer(stream, (0 until partitions).toSet)
    val data = (1 to dataSize).map(_.toByte).toArray

    val newTransaction = new ExecutionTimeMeasurement
    val send = createIf(dataSize > 0)
    val checkpoint = createIf(withCheckpoint)
    val progressBar =
      if (progressReportRate > 0) Some(new ProgressBar(iterations))
      else None

    for (i <- 1 to iterations) {
      val transaction = newTransaction(() => producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened))
      send.foreach(_.apply(() => transaction.send(data)))
      checkpoint.foreach(_.apply(() => producer.checkpoint()))

      progressBar.foreach(_.show(i, progressReportRate))
    }

    producer.close()

    ProducerBenchmark.Result(
      newTransaction = newTransaction.result,
      sendData = send.map(_.result),
      checkpoint = checkpoint.map(_.result))
  }

  def close(): Unit =
    factory.close()


  private def createIf(condition: Boolean): Option[ExecutionTimeMeasurement] = {
    if (condition) Some(new ExecutionTimeMeasurement)
    else None
  }
}


object ProducerBenchmark extends App {

  case class Result(newTransaction: ExecutionTimeMeasurement.Result,
                    sendData: Option[ExecutionTimeMeasurement.Result] = None,
                    checkpoint: Option[ExecutionTimeMeasurement.Result] = None)

}
