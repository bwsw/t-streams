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

import com.bwsw.tstreams.agents.consumer.Offset
import com.bwsw.tstreams.agents.producer.{NewProducerTransactionPolicy, Producer}
import com.bwsw.tstreams.env.{ConfigurationOptions, TStreamsFactory}

import scala.annotation.tailrec

/**
  * Provides method to measure execution time of [[com.bwsw.tstreams.agents.producer.Producer]]'s and
  * [[com.bwsw.tstreams.agents.consumer.Consumer]]'s methods
  *
  * @param address    ZooKeeper address
  * @param token      authentication token
  * @param prefix     path to master node in ZooKeeper
  * @param stream     stream name. Stream will be created if it doesn't exists.
  * @param partitions amount of stream partitions
  * @author Pavel Tomskikh
  */
class Benchmark(address: String,
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
    * Measures execution time of [[com.bwsw.tstreams.agents.producer.Producer]]'s methods
    *
    * @param iterations            amount of measurements
    * @param dataSize              size of data sent in each transaction
    * @param cancelEachTransaction if true, each transaction will be cancelled,
    *                              otherwise a checkpoint will be performed for each transaction
    * @param progressReportRate    progress will be printed to the console after this number of iterations
    * @return measurement result
    */
  def testProducer(iterations: Int,
                   dataSize: Int = 100,
                   cancelEachTransaction: Boolean = false,
                   progressReportRate: Int = 1000,
                   warmingUpIterations: Int = 5000): ProducerBenchmark.Result = {
    val producer = factory.getProducer(stream, (0 until partitions).toSet)
    val data = (1 to dataSize).map(_.toByte).toArray


    def inner(iterations: Int, maybeProgressBar: Option[ProgressBar]) = {
      val newTransaction = new ExecutionTimeMeasurement
      val send = createTimeMeasurementIf(dataSize > 0)
      val checkpoint = createTimeMeasurementIf(!cancelEachTransaction)
      val cancel = createTimeMeasurementIf(cancelEachTransaction)

      for (i <- 1 to iterations) {
        val transaction = newTransaction(() => producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened))
        send.foreach(_.apply(() => transaction.send(data)))
        checkpoint.foreach(_.apply(() => producer.checkpoint()))
        cancel.foreach(_.apply(() => producer.cancel()))

        maybeProgressBar.foreach(_.show(i, progressReportRate))
      }

      ProducerBenchmark.Result(
        newTransaction = newTransaction.result,
        sendData = send.map(_.result),
        checkpoint = checkpoint.map(_.result),
        cancel = cancel.map(_.result))
    }


    inner(warmingUpIterations, createIf(progressReportRate > 0, new ProgressBar(warmingUpIterations, "warming up")))
    val result = inner(iterations, createIf(progressReportRate > 0, new ProgressBar(iterations)))

    producer.close()

    result
  }


  /**
    * Measures execution time of methods [[com.bwsw.tstreams.agents.consumer.Consumer.getTransaction()]] and
    * [[com.bwsw.tstreams.agents.consumer.ConsumerTransaction.getAll]]
    *
    * @param iterations         amount of measurements
    * @param partition          stream partition
    * @param dataSize           size of data sent in each transaction
    * @param loadData           if true, data will be retrieved from transactions
    * @param progressReportRate progress will be printed to the console after this number of iterations
    * @return measurement result
    */
  def testGetTransaction(iterations: Int,
                         partition: Int = 0,
                         dataSize: Int = 100,
                         loadData: Boolean = false,
                         progressReportRate: Int = 1000,
                         warmingUpIterations: Int = 5000): ConsumerBenchmark.Result = {
    val producer = factory.getProducer(stream, Set(partition))

    loadTransactions(
      iterations + warmingUpIterations,
      producer,
      partition,
      dataSize,
      createIf(progressReportRate > 0, new ProgressBar(iterations + warmingUpIterations, "preparation")),
      progressReportRate)

    producer.close()

    val consumer = factory.getConsumer(stream, Set(partition), Offset.Oldest)
    consumer.start()

    def inner(iterations: Int, maybeProgressBar: Option[ProgressBar]) = {
      val getTransaction = new ExecutionTimeMeasurement
      val getTransactionData = createTimeMeasurementIf(loadData)

      @tailrec
      def iteration(i: Int): Unit = {
        if (i <= iterations) {
          val maybeTransaction = getTransaction(() => consumer.getTransaction(partition))

          maybeTransaction match {
            case Some(transaction) =>
              getTransactionData.foreach(_.apply(() => transaction.getAll))
              maybeProgressBar.foreach(_.show(i, progressReportRate))
              iteration(i + 1)

            case None =>
              maybeProgressBar.foreach(_.show(i))
              println()
              println("CAN'T GET TRANSACTION")
          }
        }
      }

      iteration(1)

      ConsumerBenchmark.Result(
        getTransaction = Some(getTransaction.result),
        getTransactionData = getTransactionData.map(_.result))
    }


    inner(warmingUpIterations, createIf(progressReportRate > 0, new ProgressBar(warmingUpIterations, "warming up")))
    val result = inner(iterations, createIf(progressReportRate > 0, new ProgressBar(iterations)))

    consumer.close()

    result
  }


  /**
    * Measures execution time of methods [[com.bwsw.tstreams.agents.consumer.Consumer.getTransactionsFromTo()]] and
    * [[com.bwsw.tstreams.agents.consumer.ConsumerTransaction.getAll]]
    *
    * @param iterations         amount of measurements
    * @param interval           amount of transaction retrieved by one invocation of
    *                           [[com.bwsw.tstreams.agents.consumer.Consumer.getTransactionsFromTo()]]
    * @param partition          stream partition
    * @param dataSize           size of data sent to each transaction
    * @param loadData           if true, data will be retrieved from transactions
    * @param progressReportRate progress will be printed to the console after this number of iterations
    * @return measurement result
    */
  def testGetTransactionsFromTo(iterations: Int,
                                interval: Int,
                                partition: Int = 0,
                                dataSize: Int = 100,
                                loadData: Boolean = false,
                                progressReportRate: Int = 1000,
                                warmingUpIterations: Int = 5000): ConsumerBenchmark.Result = {
    val producer = factory.getProducer(stream, Set(partition))

    def prepare(iterations: Int, maybeProgressBar: Option[ProgressBar]) = {
      loadTransactions(
        iterations,
        producer,
        partition,
        dataSize,
        maybeProgressBar,
        progressReportRate)
        .grouped(interval)
        .map(s => (s.head, s.last))
        .toList
    }

    val warmingUpIntervals = prepare(
      warmingUpIterations,
      createIf(progressReportRate > 0, new ProgressBar(warmingUpIterations, "warming up preparation")))

    val intervals = prepare(
      iterations,
      createIf(progressReportRate > 0, new ProgressBar(iterations, "preparation")))

    producer.close()

    val consumer = factory.getConsumer(stream, Set(partition), Offset.Oldest)
    consumer.start()


    def inner(iterations: Int, intervals: Seq[(Long, Long)], maybeProgressBar: Option[ProgressBar]) = {
      val getTransactionFromTo = new ExecutionTimeMeasurement
      val getTransactionData = createTimeMeasurementIf(loadData)
      var i = 0

      intervals.foreach {
        case (from, to) =>
          val transactions = getTransactionFromTo(() => consumer.getTransactionsFromTo(partition, from - 1, to))
          transactions.foreach { transaction =>
            getTransactionData.foreach(_.apply(() => transaction.getAll))
            i += 1
            maybeProgressBar.foreach(_.show(i, progressReportRate))
          }
      }

      if (i < iterations) {
        maybeProgressBar.foreach(_.show(i))
        println()
      }

      ConsumerBenchmark.Result(
        getTransactionFromTo = Some(getTransactionFromTo.result),
        getTransactionData = getTransactionData.map(_.result))
    }


    inner(
      warmingUpIterations,
      warmingUpIntervals,
      createIf(progressReportRate > 0, new ProgressBar(warmingUpIterations, "warming up")))

    val result = inner(
      iterations,
      intervals,
      createIf(progressReportRate > 0, new ProgressBar(iterations)))

    consumer.close()

    result
  }


  def close(): Unit =
    factory.close()


  private def loadTransactions(transactions: Int,
                               producer: Producer,
                               partition: Int,
                               dataSize: Int = 100,
                               maybeProgressBar: Option[ProgressBar],
                               progressReportRate: Int): IndexedSeq[Long] = {
    val data = (1 to dataSize).map(_.toByte).toArray

    (1 to transactions).map { i =>
      val transaction = producer.newTransaction(NewProducerTransactionPolicy.ErrorIfOpened, partition)
      transaction.send(data)
      producer.checkpoint()

      maybeProgressBar.foreach(_.show(i, progressReportRate))

      transaction
    }.map(_.getTransactionID)
  }

  private def createTimeMeasurementIf(condition: Boolean): Option[ExecutionTimeMeasurement] =
    createIf(condition, new ExecutionTimeMeasurement)

  private def createIf[T](condition: Boolean, constructor: => T) =
    if (condition) Some(constructor) else None
}

object Benchmark {

  trait Result {
    def toMap: Map[String, ExecutionTimeMeasurement.Result]
  }

}