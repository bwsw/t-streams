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
package com.bwsw.tstreamstransactionserver.netty.server.db.zk

import com.bwsw.tstreamstransactionserver.netty.server.streamService
import com.bwsw.tstreamstransactionserver.netty.server.streamService.StreamRecord
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.locks.{InterProcessReadWriteLock, InterProcessSemaphoreMutex}
import org.apache.zookeeper.CreateMode
import org.slf4j.LoggerFactory

import scala.util.Try

final class StreamNamePath(client: CuratorFramework, path: String) {
  private val logger =
    LoggerFactory.getLogger(this.getClass)

  private val lock =
    new InterProcessSemaphoreMutex(client, path)

  private def writeLock[T](body: => T) = {
    lock.acquire()
    val result =
      try {
        body
      }
      finally {
        lock.release()
      }
    result
  }

  private def readLock[T](body: => T) = {
    lock.acquire()
    val result =
      try {
        body
      }
      finally {
        lock.release()
      }
    result
  }

  def exists(streamName: String): Boolean =
    readLock {
      Try(client.checkExists().forPath(s"$path/$streamName")) match {
        case scala.util.Success(stat) if stat != null => true
        case _ => false
      }
    }

  def get(streamName: String): Option[streamService.StreamRecord] =
    readLock {
      scala.util.Try(client.getData.forPath(s"$path/$streamName"))
        .toOption.map(data => StreamRecord.fromBinaryJson(data))
    }


  def put(streamRecord: streamService.StreamRecord): Unit =
    writeLock {
      client.create()
        .creatingParentsIfNeeded()
        .withMode(CreateMode.PERSISTENT)
        .forPath(
          s"$path/${streamRecord.name}",
          streamRecord.toBinaryJson
        )

      if (logger.isDebugEnabled)
        logger.debug(s"Persisted stream: " +
          s"id ${streamRecord.id}, " +
          s"name ${streamRecord.name}, " +
          s"partitions ${streamRecord.partitions}, " +
          s"description ${streamRecord.description}, " +
          s"ttl ${streamRecord.ttl}"
        )
    }

  def delete(streamName: String): Boolean =
    writeLock {
      val isDeleted = scala.util.Try(client.delete().forPath(s"$path/$streamName"))
        .isSuccess

      if (isDeleted && logger.isDebugEnabled) {
        logger.debug(s"Stream name $streamName is ${if (isDeleted) "" else "not"} deleted")
      }

      isDeleted
    }
}