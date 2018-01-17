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


import org.apache.curator.framework.CuratorFramework

import scala.annotation.tailrec

/**
  * Class is used to keep metadata of related entities. Metadata represents a set of znodes.
  * The main node is a root node containing two elements: id of the first and last existing entities.
  * The rest nodes are created under the root node.
  * These nodes keep only one id that is a kind of link to the next entity
  *
  * @param client   zookeeper client
  * @param rootPath node path
  * @tparam T id type
  */
abstract class ZookeeperTreeList[T](client: CuratorFramework,
                                    rootPath: String)
  extends EntityPathConverter[T]
    with EntityIdSerializable[T] {
  private val rootNode = new RootNode(client, rootPath)

  def firstEntityId: Option[T] = {
    val rootNodeData = rootNode.getData()
    val binaryId = rootNodeData.firstId
    if (binaryId.isEmpty)
      None
    else
      Some(bytesToEntityId(binaryId))
  }

  def createNode(entity: T): Unit = {
    val lastId = entityIdToBytes(entity)
    val path = buildPath(entity)

    def persistNode() = {
      client.create
        .creatingParentsIfNeeded()
        .forPath(
          path,
          Array.emptyByteArray
        )
    }

    val rootNodeData = rootNode.getData()
    if (rootNodeData.firstId.isEmpty) {
      persistNode()
      rootNode.setData(
        lastId, lastId
      )
    }
    else if (bytesToEntityId(rootNodeData.lastId) != entity) {
      persistNode()

      traverseToLastNode.foreach { id =>
        val pathPreviousNode = buildPath(id)
        client.setData()
          .forPath(
            pathPreviousNode,
            lastId
          )
      }

      rootNode.setData(
        rootNodeData.firstId, lastId
      )
    }
  }


  private def traverseToLastNode: Option[T] = {
    @tailrec
    def go(node: Option[T]): Option[T] = {
      val nodeId = node.flatMap(id =>
        getNextNode(id)
      )

      if (nodeId.isDefined)
        go(nodeId)
      else
        node
    }

    go(lastEntityId)
  }

  def lastEntityId: Option[T] = {
    val rootNodeData = rootNode.getData()
    val binaryId = rootNodeData.lastId
    if (binaryId.isEmpty)
      None
    else
      Some(bytesToEntityId(binaryId))
  }

  def getNextNode(entity: T): Option[T] = {
    val path = buildPath(entity)
    val data = client.getData
      .forPath(path)

    if (data.isEmpty)
      None
    else
      Some(bytesToEntityId(data))
  }

  private def buildPath(entity: T) = {
    s"$rootPath/${entityToPath(entity).mkString("/")}"
  }

  def deleteNode(id: T): Boolean = {
    val firstIdOpt = firstEntityId
    val lastIdOpt = lastEntityId

    firstIdOpt -> lastIdOpt match {
      case (Some(firstId), Some(lastId)) =>
        if (id == firstId && id == lastId) {
          deleteOneNodeTreeList(id)
        }
        else {
          if (id == firstId) {
            val nextId = getNextNode(id).get
            deleteFirstNode(firstId, nextId)
          }
          else if (id == lastId) {
            val previousId = getPreviousNode(id).get
            deleteLastNode(lastId, previousId)
          }
          else {
            for {
              nextId <- getNextNode(id)
              previousId <- getPreviousNode(id)
            } yield {
              if (updateNode(previousId, nextId))
                scala.util.Try(
                  client.delete().forPath(buildPath(id))
                ).isSuccess
              else
                false
            }
          }.getOrElse(false)
        }
      case _ =>
        false
    }
  }

  @tailrec
  final def deleteLeftNodes(number: Long): Unit = {
    if (number > 0L) {
      val firstIdOpt = firstEntityId
      firstIdOpt match {
        case Some(firstId) =>
          deleteNode(firstId)
          deleteLeftNodes(number - 1)
        case None => //deletion is completed
      }
    }
  }

  def getPreviousNode(entity: T): Option[T] = {
    @tailrec
    def go(node: Option[T]): Option[T] = {
      val nodeId = node.flatMap(id =>
        getNextNode(id).filter(_ != entity)
      )
      if (nodeId.isDefined)
        go(nodeId)
      else
        node
    }

    go(firstEntityId).flatMap(previousNodeId =>
      if (lastEntityId.contains(previousNodeId))
        None
      else
        Some(previousNodeId)
    )
  }

  private def deleteFirstNode(firstEntityId: T, nextEntityId: T): Boolean = {
    val newFirstId = entityIdToBytes(nextEntityId)

    val rootNodeData = rootNode.getData()
    val lastId = rootNodeData.lastId
    rootNode.setData(
      newFirstId,
      lastId
    )

    scala.util.Try(
      client.delete().forPath(buildPath(firstEntityId))
    ).isSuccess
  }

  private def deleteLastNode(lastEntityId: T, previousEntityId: T): Boolean = {
    val newLastId = entityIdToBytes(previousEntityId)

    val rootNodeData = rootNode.getData()
    val firstId = rootNodeData.firstId
    rootNode.setData(
      firstId,
      newLastId
    )

    updateNode(previousEntityId, Array.emptyByteArray) &&
      scala.util.Try(
        client.delete().forPath(buildPath(lastEntityId))
      ).isSuccess
  }

  private def deleteOneNodeTreeList(id: T): Boolean = {
    rootNode.clear()

    scala.util.Try(
      client.delete().forPath(buildPath(id))
    ).isSuccess
  }

  private def updateNode(nodeId: T, nextNodeId: T): Boolean = {
    scala.util.Try(client.setData()
      .forPath(
        buildPath(nodeId),
        entityIdToBytes(nextNodeId)
      )
    ).isSuccess
  }

  private def updateNode(nodeId: T, nextNodeId: Array[Byte]): Boolean = {
    scala.util.Try(client.setData()
      .forPath(
        buildPath(nodeId),
        nextNodeId
      )
    ).isSuccess
  }
}