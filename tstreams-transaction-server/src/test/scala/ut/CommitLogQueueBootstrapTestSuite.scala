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

package ut

import java.nio.file.Paths
import java.util.concurrent.PriorityBlockingQueue

import com.bwsw.commitlog.filesystem.{CommitLogCatalogue, CommitLogStorage}
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.CommitLogQueueBootstrap
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import util.Utils.startZkServerAndGetIt

import scala.collection.mutable.ArrayBuffer

class CommitLogQueueBootstrapTestSuite
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll {

  //arrange
  private lazy val (zkServer, zkClient) = startZkServerAndGetIt
  private lazy val bundle = util.Utils.getTransactionServerBundle(zkClient)
  private lazy val storageOptions = bundle.storageOptions
  private lazy val path = Paths.get(storageOptions.path, storageOptions.commitLogRawDirectory).toString
  private lazy val commitLogCatalogue = new CommitLogCatalogue(path)
  private lazy val commitLogQueueBootstrap =
    new CommitLogQueueBootstrap(10, commitLogCatalogue, bundle.singleNodeCommitLogService)

  "fillQueue" should "return an empty queue if there are no commit log files in a storage directory" in {
    //act
    val emptyQueue = commitLogQueueBootstrap.fillQueue()

    //assert
    emptyQueue shouldBe empty
  }

  it should "return a queue of a size that equals to a number of commit log files are in a storage directory" in {
    //arrange
    val numberOfFiles = 10
    createCommitLogFiles(numberOfFiles)

    //act
    val nonemptyQueue = commitLogQueueBootstrap.fillQueue()

    //assert
    nonemptyQueue should have size numberOfFiles
  }

  it should "return a queue with the time ordered commit log files" in {
    //arrange
    val numberOfFiles = 1
    createCommitLogFiles(numberOfFiles)
    createCommitLogFiles(numberOfFiles)
    createCommitLogFiles(numberOfFiles)

    //act
    val orderedQueue = commitLogQueueBootstrap.fillQueue()
    val orderedFiles = getOrderedFiles(orderedQueue)

    //assert
    orderedFiles shouldBe sorted
  }

  override def afterAll(): Unit = {
    zkClient.close()
    zkServer.close()
    bundle.closeDbsAndDeleteDirectories()
  }

  private def createCommitLogFiles(number: Int): Unit = {
    (0 until number).foreach(fileNamePrefix => {
      commitLogCatalogue.createFile(fileNamePrefix.toString)
    })
  }

  private def getOrderedFiles(orderedQueue: PriorityBlockingQueue[CommitLogStorage]): ArrayBuffer[CommitLogStorage] = {
    val orderedFiles = ArrayBuffer[CommitLogStorage]()
    var path = orderedQueue.poll()
    while (path != null) {
      orderedFiles += path
      path = orderedQueue.poll()
    }

    orderedFiles
  }
}
