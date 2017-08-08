package com.bwsw.tstreamstransactionserver.netty.client.zk

import com.bwsw.tstreamstransactionserver.exception.Throwable.{MasterDataIsIllegalException, MasterIsPersistentZnodeException}
import com.bwsw.tstreamstransactionserver.netty.SocketHostPortPair
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.api.{CuratorEvent, CuratorListener}
import org.apache.curator.framework.recipes.cache.{ChildData, NodeCache, NodeCacheListener}
import org.apache.curator.framework.state.ConnectionState
import org.slf4j.LoggerFactory

private object ZKMasterPathMonitor {
  val NonEphemeralNode = 0L
}

class ZKMasterPathMonitor(connection: CuratorFramework,
                          prefix: String,
                          setMaster: Either[Throwable, Option[SocketHostPortPair]] => Unit)
  extends NodeCacheListener
    with CuratorListener {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val nodeToWatch = new NodeCache(
    connection,
    prefix,
    false
  )
  @volatile private var isClosed = true

  override def nodeChanged(): Unit = {
    Option(nodeToWatch.getCurrentData) match {
      case Some(node) =>
        if (node.getStat.getEphemeralOwner == ZKMasterPathMonitor.NonEphemeralNode)
          setMaster(Left(new MasterIsPersistentZnodeException(node.getPath)))
        else
          setMaster(validateMaster(node))
      case None =>
        setMaster(Right(None))
      //        setMaster(
      //          Left(
      //            throw new MasterPathIsAbsent(prefix)
      //          )
      //        )
    }
  }

  private def validateMaster(node: ChildData) = {
    val hostPort = new String(node.getData)
    val connectionData = connection
      .getZookeeperClient.getCurrentConnectionString

    SocketHostPortPair.fromString(hostPort) match {
      case Some(hostPortPairOpt) =>
        Right(Some(hostPortPairOpt))
      case None =>
        if (logger.isErrorEnabled()) {
          logger.error(s"Master information data ($hostPort) is corrupted for $connectionData$prefix.")
        }
        Left(new MasterDataIsIllegalException(node.getPath, hostPort))
    }
  }

  override def eventReceived(client: CuratorFramework,
                             event: CuratorEvent): Unit = {
    event match {
      case ConnectionState.LOST =>
        setMaster(Right(None))
      case _ =>
        ()
    }
  }

  def startMonitoringMasterServerPath(): Unit =
    this.synchronized {
      if (isClosed) {
        isClosed = false
        nodeToWatch.getListenable.addListener(this)
        connection.getCuratorListenable.addListener(this)
        nodeToWatch.start()
      }
    }

  def stopMonitoringMasterServerPath(): Unit =
    this.synchronized {
      if (!isClosed) {
        isClosed = true
        nodeToWatch.getListenable.removeListener(this)
        connection.getCuratorListenable.removeListener(this)
        nodeToWatch.close()
      }
    }
}
