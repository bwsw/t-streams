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

package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeeperService.hierarchy.LongZookeeperTreeList
import com.bwsw.tstreamstransactionserver.util.Utils
import com.bwsw.tstreamstransactionserver.util.Utils.uuid
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class ZookeeperTreeListTest
  extends FlatSpec
    with BeforeAndAfterAll
    with Matchers {

  private lazy val (zkServer, zkClient) = Utils.startZkServerAndGetIt

  override def afterAll(): Unit = {
    zkClient.close()
    zkServer.close()
  }


  "ZookeeperTreeListLong" should "return first entry id and last entry id as Nones" in {
    val treeListLong = new LongZookeeperTreeList(zkClient, s"/$uuid")

    treeListLong.lastEntityId shouldBe None
    treeListLong.firstEntityId shouldBe None
  }

  it should "return first entry id and last entry id the same as only one entity id was persisted" in {
    val treeListLong = new LongZookeeperTreeList(zkClient, s"/$uuid")

    val value = 1L
    treeListLong.createNode(value)

    treeListLong.firstEntityId shouldBe defined
    treeListLong.firstEntityId.get == value

    treeListLong.firstEntityId shouldBe treeListLong.lastEntityId
  }

  it should "return first entry id and last entry id properly" in {
    val treeListLong = new LongZookeeperTreeList(zkClient, s"/$uuid")

    val startNumber = 0
    val maxNumbers = 10

    (startNumber to maxNumbers)
      .foreach(number => treeListLong.createNode(number))

    treeListLong.firstEntityId shouldBe Some(startNumber)
    treeListLong.lastEntityId shouldBe Some(maxNumbers)
  }

  it should "return a next node of some node properly" in {
    val treeListLong = new LongZookeeperTreeList(zkClient, s"/$uuid")

    val startNumber = 0
    val maxNumbers = 10

    (startNumber to maxNumbers)
      .foreach(number => treeListLong.createNode(number))

    val id = scala.util.Random.nextInt(maxNumbers)
    treeListLong.getNextNode(id) shouldBe Some(id + 1)
  }

  it should "not return a next node of last node" in {
    val treeListLong = new LongZookeeperTreeList(zkClient, s"/$uuid")

    val startNumber = 0
    val maxNumbers = 10

    (startNumber to maxNumbers)
      .foreach(number => treeListLong.createNode(number))

    val id = maxNumbers
    treeListLong.getNextNode(id) shouldBe None
  }

  it should "return a previous node of some node correctly" in {
    val treeListLong = new LongZookeeperTreeList(zkClient, s"/$uuid")

    val startNumber = 0
    val maxNumbers = 10

    (startNumber to maxNumbers)
      .foreach(number => treeListLong.createNode(number))

    val id = scala.util.Random.nextInt(maxNumbers) + 1
    treeListLong.getPreviousNode(id) shouldBe Some(id - 1)
  }

  it should "not return a previous node that doesn't exit" in {
    val treeListLong = new LongZookeeperTreeList(zkClient, s"/$uuid")

    val startNumber = 0
    val maxNumbers = 10

    (startNumber to maxNumbers)
      .foreach(number => treeListLong.createNode(number))

    val id = -1
    treeListLong.getPreviousNode(id) shouldBe None
  }

  it should "delete the one node tree list correctly" in {
    val treeListLong = new LongZookeeperTreeList(zkClient, s"/$uuid")

    val startNumber = 0
    val maxNumbers = 0

    val ids = (startNumber to maxNumbers).toArray

    ids.foreach(id =>
      treeListLong.createNode(id)
    )

    val id = 0
    treeListLong.deleteNode(id) shouldBe true

    treeListLong.firstEntityId shouldBe None
    treeListLong.lastEntityId shouldBe None
  }

  it should "delete the first node correctly" in {
    val treeListLong = new LongZookeeperTreeList(zkClient, s"/$uuid")

    val startNumber = 0
    val maxNumbers = 7

    val ids = (startNumber to maxNumbers).toArray

    ids.foreach(id =>
      treeListLong.createNode(id)
    )

    val firstID = startNumber
    val nextID = startNumber + 1
    treeListLong.deleteNode(firstID) shouldBe true

    treeListLong.firstEntityId shouldBe Some(nextID)
    treeListLong.lastEntityId shouldBe Some(maxNumbers)
  }

  it should "delete the last node correctly" in {
    val treeListLong = new LongZookeeperTreeList(zkClient, s"/$uuid")

    val startNumber = 0
    val maxNumbers = 7

    val ids = (startNumber to maxNumbers).toArray

    ids.foreach(id =>
      treeListLong.createNode(id)
    )

    val lastID = maxNumbers
    val previousID = lastID - 1
    treeListLong.deleteNode(lastID) shouldBe true
    treeListLong.getNextNode(previousID) shouldBe None

    treeListLong.firstEntityId shouldBe Some(startNumber)
    treeListLong.lastEntityId shouldBe Some(previousID)
  }


  it should "delete a node between first entity id and last entity id" in {
    val treeListLong = new LongZookeeperTreeList(zkClient, s"/$uuid")

    val startNumber = 0
    val maxNumbers = 10
    val moveRangeToLeftBoundNumber = 2

    val ids = (startNumber to maxNumbers).toArray

    ids.foreach(id =>
      treeListLong.createNode(id)
    )

    val id = scala.util.Random.nextInt(
      maxNumbers - moveRangeToLeftBoundNumber
    ) + moveRangeToLeftBoundNumber

    val previousID = id - 1
    val nextID = id + 1
    treeListLong.deleteNode(id) shouldBe true
    treeListLong.getNextNode(previousID - 1) shouldBe Some(previousID)
    treeListLong.getNextNode(previousID) shouldBe Some(nextID)

    treeListLong.firstEntityId shouldBe Some(startNumber)
    treeListLong.lastEntityId shouldBe Some(maxNumbers)
  }

  it should "delete nodes from [head, n], n - some positive number" in {
    val treeListLong = new LongZookeeperTreeList(zkClient, s"/$uuid")

    val startNumber = 0
    val maxNumbers = 30

    val ids = (startNumber to maxNumbers).toArray

    ids.foreach(id =>
      treeListLong.createNode(id)
    )

    val number = scala.util.Random.nextInt(maxNumbers)
    treeListLong.deleteLeftNodes(number)

    treeListLong.firstEntityId shouldBe Some(number)
    treeListLong.lastEntityId shouldBe Some(maxNumbers)
  }

  it should "delete nodes from [head, n], n - some positive number, which is greater than number of nodes list contains" in {
    val treeListLong = new LongZookeeperTreeList(zkClient, s"/$uuid")

    val startNumber = 0
    val maxNumbers = 15

    val ids = (startNumber to maxNumbers).toArray

    ids.foreach(id =>
      treeListLong.createNode(id)
    )

    val number = maxNumbers + 1
    treeListLong.deleteLeftNodes(number)

    treeListLong.firstEntityId shouldBe None
    treeListLong.lastEntityId shouldBe None
  }
}
