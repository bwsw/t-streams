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

package com.bwsw.tstreams.testutils

import java.io.File
import java.lang.management.ManagementFactory
import java.util.concurrent.atomic.AtomicInteger

import com.bwsw.tstreams.agents.consumer.Consumer
import com.bwsw.tstreams.agents.producer.ProducerTransaction
import com.bwsw.tstreams.env.{ConfigurationOptions, TStreamsFactory}
import com.bwsw.tstreamstransactionserver.rpc.TransactionStates
import com.google.common.io.Files
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor2}
import org.scalatest.{Assertion, Matchers}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

/**
  * Test help utils
  */
trait TestUtils extends Matchers with TableDrivenPropertyChecks {
  protected val batchSizeTestVal = 5

  /**
    * Random alpha string generator
    *
    * @return Alpha string
    */
  val id: Int = TestUtils.moveId()
  val randomKeyspace: String = TestUtils.getKeyspace(id)

  val zookeeperPort: Int = TestUtils.ZOOKEEPER_PORT


  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val uptime: Long = ManagementFactory.getRuntimeMXBean.getStartTime

  logger.info("-------------------------------------------------------")
  logger.info("Test suite " + this.getClass.toString + " started")
  logger.info("Test Suite uptime is " + ((System.currentTimeMillis - uptime) / 1000L).toString + " seconds")
  logger.info("-------------------------------------------------------")

  val DEFAULT_STREAM_NAME = "test_stream"

  val f = new TStreamsFactory()
  f.setProperty(ConfigurationOptions.Coordination.endpoints, s"localhost:$zookeeperPort")
    .setProperty(ConfigurationOptions.Stream.name, DEFAULT_STREAM_NAME)
    .setProperty(ConfigurationOptions.Stream.partitionsCount, 3)
    .setProperty(ConfigurationOptions.Common.authenticationKey, TestUtils.AUTH_KEY)

  val curatorClient: CuratorFramework = CuratorFrameworkFactory.builder()
    .namespace("")
    .connectionTimeoutMs(7000)
    .sessionTimeoutMs(7000)
    .retryPolicy(new ExponentialBackoffRetry(1000, 3))
    .connectString(s"127.0.0.1:$zookeeperPort").build()
  curatorClient.start()

  if (Option(curatorClient.checkExists().forPath("/tts")).isEmpty)
    curatorClient.create().forPath("/tts")

  removeZkMetadata(f.getProperty(ConfigurationOptions.Coordination.path).toString)

  def getRandomString: String = RandomStringCreator.randomAlphaString(10)

  /**
    * Sorting checker
    */
  def isSorted(list: ListBuffer[Long]): Boolean = {
    var currentMax = Long.MinValue

    list.forall { value =>
      if (value < currentMax) false
      else {
        currentMax = value

        true
      }
    }
  }

  /**
    * Remove zk metadata from concrete root
    *
    * @param path Zk root to delete
    */
  def removeZkMetadata(path: String): Unit = {
    if (Option(curatorClient.checkExists.forPath(path)).isDefined)
      curatorClient.delete.deletingChildrenIfNeeded().forPath(path)
  }

  /**
    * Remove directory recursive
    *
    * @param file Dir to remove
    */
  def remove(file: File): Unit = {
    if (file.isDirectory)
      file.listFiles().foreach(remove)

    file.delete()
  }

  def onAfterAll(): Unit = {
    System.setProperty("DEBUG", "false")
    removeZkMetadata(f.getProperty(ConfigurationOptions.Coordination.path).toString)
    removeZkMetadata("/unit")
    curatorClient.close()
    f.dumpStorageClients()
  }

  def createNewStream(partitions: Int = 3, name: String = DEFAULT_STREAM_NAME): Unit = {
    val storageClient = f.getStorageClient()
    if (storageClient.checkStreamExists(name))
      storageClient.deleteStream(name)

    storageClient.createStream(name, partitions, 24 * 3600, "")
    storageClient.shutdown()
  }

  def checkTransactions(consumer: Consumer,
                        partition: Int,
                        table: TableFor2[ProducerTransaction, TransactionStates]): Assertion = {
    forAll(table) { (transaction, state) =>
      val maybeConsumerTransaction = consumer.getTransactionById(partition, transaction.getTransactionID)
      maybeConsumerTransaction shouldBe defined

      val consumerTransaction = maybeConsumerTransaction.get
      consumerTransaction.getTransactionID shouldBe transaction.getTransactionID
      consumerTransaction.getState shouldBe state
    }
  }
}


object TestUtils {
  System.getProperty("java.io.tmpdir", "./target/")
  val ZOOKEEPER_PORT = 21810

  private val id: AtomicInteger = new AtomicInteger(0)

  def moveId(): Int = id.incrementAndGet()

  def getKeyspace(id: Int): String = "tk_" + id.toString

  def getTmpDir(): String = Files.createTempDir().toString

  new ZookeeperTestServer(ZOOKEEPER_PORT, Files.createTempDir().toString)

  val AUTH_KEY = "test"
  val MASTER_PREFIX = "/tts/master"

}
