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

package com.bwsw.tstreamstransactionserver.netty.client.zk

import java.util.concurrent.atomic.AtomicBoolean

import com.bwsw.tstreamstransactionserver.exception.Throwable.{MasterDataIsIllegalException, MasterIsPersistentZnodeException, ZkNoConnectionException}
import com.bwsw.tstreamstransactionserver.netty.SocketHostPortPair
import com.bwsw.tstreamstransactionserver.netty.client.MasterReelectionListener
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.api.{CuratorEvent, CuratorListener}
import org.apache.curator.framework.recipes.cache.{ChildData, NodeCache, NodeCacheListener}
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.slf4j.LoggerFactory

import scala.annotation.tailrec

private object ZKMasterPathMonitor {
  val NonEphemeralNode = 0L
}

class ZKMasterPathMonitor(connection: CuratorFramework,
                          retryDelayMs: Int,
                          prefix: String)
  extends NodeCacheListener
    with CuratorListener {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val isClosed = new AtomicBoolean(true)

  private val listeners =
    java.util.concurrent.ConcurrentHashMap.newKeySet[MasterReelectionListener]()

  private val nodeToWatch = new NodeCache(
    connection,
    prefix,
    false
  )

  private var master: Either[Throwable, Option[SocketHostPortPair]] = Right(None)

  final def getCurrentMaster: Either[Throwable, Option[SocketHostPortPair]] = master

  final def getMasterInBlockingManner: Either[Throwable, SocketHostPortPair] = {
    @tailrec
    def go(timestamp: Long): Either[Throwable, SocketHostPortPair] = {
      master match {
        case Left(throwable) =>
          if (logger.isWarnEnabled())
            logger.warn(throwable.getMessage)
          Left(throwable)
        case Right(masterOpt) =>
          if (masterOpt.isDefined) {
            Right(masterOpt.get)
          }
          else {
            val currentTime = System.currentTimeMillis()
            val timeDiff = scala.math.abs(currentTime - timestamp)
            if (logger.isInfoEnabled && timeDiff >= retryDelayMs) {
              logger.info(s"Retrying to get master server from " +
                s"zookeeper servers: ${connection.getZookeeperClient.getCurrentConnectionString}."
              )
              go(currentTime)
            }
            else
              go(timestamp)
          }
      }
    }

    go(System.currentTimeMillis())
  }

  private def validateMaster(node: ChildData) = {
    val hostPort = new String(node.getData)

    val connectionData = connection
      .getZookeeperClient.getCurrentConnectionString

    SocketHostPortPair.fromString(hostPort)
      .map(hostPortPairOpt => Right(Some(hostPortPairOpt)))
      .getOrElse {
        if (logger.isErrorEnabled()) {
          logger.error(s"Master information data ($hostPort) is corrupted for $connectionData$prefix.")
        }
        Left(new MasterDataIsIllegalException(node.getPath, hostPort))
      }
  }

  override def nodeChanged(): Unit = {
    val newMaster = Option(nodeToWatch.getCurrentData)
      .map(node =>
        if (node.getStat.getEphemeralOwner == ZKMasterPathMonitor.NonEphemeralNode)
          Left(new MasterIsPersistentZnodeException(node.getPath))
        else
          validateMaster(node)
      ).getOrElse(Right(None))

    master = newMaster
    listeners.forEach(_.masterChanged(newMaster))
  }

  override def eventReceived(client: CuratorFramework,
                             event: CuratorEvent): Unit = {
    event.getWatchedEvent.getState match {
      case KeeperState.Disconnected =>
        val newMaster = Left(
          new ZkNoConnectionException(client.getZookeeperClient.getCurrentConnectionString))

        master = newMaster
        listeners.forEach(_.masterChanged(newMaster))

      case _ =>
    }
  }

  def addMasterReelectionListener(listener: MasterReelectionListener): Unit = listeners.add(listener)

  def removeMasterReelectionListener(listener: MasterReelectionListener): Unit = listeners.remove(listener)

  def startMonitoringMasterServerPath(): Unit = {
    if (isClosed.getAndSet(false)) {
      nodeToWatch.getListenable.addListener(this)
      connection.getCuratorListenable.addListener(this)
      nodeToWatch.start()
    }
  }

  def stopMonitoringMasterServerPath(): Unit = {
    if (!isClosed.getAndSet(true)) {
      nodeToWatch.getListenable.removeListener(this)
      connection.getCuratorListenable.removeListener(this)
      nodeToWatch.close()
    }
  }
}
