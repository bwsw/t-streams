package com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy


import org.apache.curator.framework.CuratorFramework

import scala.annotation.tailrec

abstract class ZookeeperTreeList[T](client: CuratorFramework,
                                    rootPath: String)
  extends EntityPathConverter[T]
    with EntityIDSerializable[T] {
  private val rootNode = new RootNode(client, rootPath)

  def firstEntityID: Option[T] = {
    val rootNodeData = rootNode.getCurrentData
    val binaryID = rootNodeData.firstID
    if (binaryID.isEmpty)
      None
    else
      Some(bytesToEntityID(binaryID))
  }

  def cachedFirstEntityID: Option[T] = {
    val binaryID = rootNode
      .getLocalCachedCurrentData
      .firstID
    if (binaryID.isEmpty)
      None
    else
      Some(bytesToEntityID(binaryID))
  }


  def createNode(entity: T): Unit = {
    val lastID = entityIDtoBytes(entity)
    val path = buildPath(entity)

    def persistNode() = {
      client.create
        .creatingParentsIfNeeded()
        .forPath(
          path,
          Array.emptyByteArray
        )
    }

    val rootNodeData = rootNode.getCurrentData
    if (rootNodeData.firstID.isEmpty) {
      persistNode()
      rootNode.setFirstAndLastIDInRootNode(
        lastID, lastID
      )
    }
    else if (bytesToEntityID(rootNodeData.lastID) != entity) {
      persistNode()

      traverseToLastNode.foreach { id =>
        val pathPreviousNode = buildPath(id)
        client.setData()
          .forPath(
            pathPreviousNode,
            lastID
          )
      }

      rootNode.setFirstAndLastIDInRootNode(
        rootNodeData.firstID, lastID
      )
    }
  }


  private def traverseToLastNode: Option[T] = {
    @tailrec
    def go(node: Option[T]): Option[T] = {
      val nodeID = node.flatMap(id =>
        getNextNode(id)
      )

      if (nodeID.isDefined)
        go(nodeID)
      else
        node
    }

    go(lastEntityID)
  }

  def lastEntityID: Option[T] = {
    val rootNodeData = rootNode.getCurrentData
    val binaryID = rootNodeData.lastID
    if (binaryID.isEmpty)
      None
    else
      Some(bytesToEntityID(binaryID))
  }

  def cachedLastEntityID: Option[T] = {
    val binaryID = rootNode
      .getLocalCachedCurrentData
      .lastID
    if (binaryID.isEmpty)
      None
    else
      Some(bytesToEntityID(binaryID))
  }



  def getNextNode(entity: T): Option[T] = {
    val path = buildPath(entity)
    val data = client.getData
      .forPath(path)

    if (data.isEmpty)
      None
    else
      Some(bytesToEntityID(data))
  }

  private def buildPath(entity: T) = {
    s"$rootPath/${entityToPath(entity).mkString("/")}"
  }

  def deleteNode(id: T): Boolean = {
    val firstIDOpt = firstEntityID
    val lastIDOpt = lastEntityID

    firstIDOpt -> lastIDOpt match {
      case (Some(firstID), Some(lastID)) =>
        if (id == firstID && id == lastID) {
          deleteOneNodeTreeList(id)
        }
        else {
          if (id == firstID) {
            val nextID = getNextNode(id).get
            deleteFirstNode(firstID, nextID)
          }
          else if (id == lastID) {
            val previousID = getPreviousNode(id).get
            deleteLastNode(lastID, previousID)
          }
          else {
            for {
              nextID <- getNextNode(id)
              previousID <- getPreviousNode(id)
            } yield {
              if (updateNode(previousID, nextID))
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
      val firstIDOpt = firstEntityID
      firstIDOpt match {
        case Some(firstID) =>
          deleteNode(firstID)
          deleteLeftNodes(number - 1)
        case None => //do nothing
      }
    }
  }

  def getPreviousNode(entity: T): Option[T] = {
    @tailrec
    def go(node: Option[T]): Option[T] = {
      val nodeID = node.flatMap(id =>
        getNextNode(id).filter(_ != entity)
      )
      if (nodeID.isDefined)
        go(nodeID)
      else
        node
    }

    go(firstEntityID).flatMap(previousNodeID =>
      if (lastEntityID.contains(previousNodeID))
        None
      else
        Some(previousNodeID)
    )
  }

  private def deleteFirstNode(firstEntityID: T, nextEntityID: T): Boolean = {
    val newFirstID = entityIDtoBytes(nextEntityID)

    val rootNodeData = rootNode.getCurrentData
    val lastID = rootNodeData.lastID
    rootNode.setFirstAndLastIDInRootNode(
      newFirstID,
      lastID
    )

    scala.util.Try(
      client.delete().forPath(buildPath(firstEntityID))
    ).isSuccess
  }

  private def deleteLastNode(lastEntityID: T, previousEntityID: T): Boolean = {
    val newLastID = entityIDtoBytes(previousEntityID)

    val rootNodeData = rootNode.getCurrentData
    val firstID = rootNodeData.firstID
    rootNode.setFirstAndLastIDInRootNode(
      firstID,
      newLastID
    )

    updateNode(previousEntityID, Array.emptyByteArray) &&
      scala.util.Try(
        client.delete().forPath(buildPath(lastEntityID))
      ).isSuccess
  }

  private def deleteOneNodeTreeList(id: T): Boolean = {
    rootNode.setFirstAndLastIDInRootNode(
      Array.emptyByteArray,
      Array.emptyByteArray
    )

    scala.util.Try(
      client.delete().forPath(buildPath(id))
    ).isSuccess
  }

  private def updateNode(nodeID: T, nextNodeID: T): Boolean = {
    scala.util.Try(client.setData()
      .forPath(
        buildPath(nodeID),
        entityIDtoBytes(nextNodeID)
      )
    ).isSuccess
  }

  private def updateNode(nodeID: T, nextNodeID: Array[Byte]): Boolean = {
    scala.util.Try(client.setData()
      .forPath(
        buildPath(nodeID),
        nextNodeID
      )
    ).isSuccess
  }
}