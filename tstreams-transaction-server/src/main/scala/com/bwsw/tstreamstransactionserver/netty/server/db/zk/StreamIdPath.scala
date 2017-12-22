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
import com.bwsw.tstreamstransactionserver.netty.server.streamService.{StreamKey, StreamRecord}
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode
//import org.slf4j.LoggerFactory

final class StreamIdPath(client: CuratorFramework, path: String) {
  //  private val logger = LoggerFactory.getLogger(this.getClass)

  private val seqPrefix = "id"
  private val streamsIdsPath = s"$path/$seqPrefix"

  private def getIDFromPath(pathWithId: String): String = pathWithId.splitAt(
    path.length + seqPrefix.length + 1
  )._2

  private def buildPath(streamID: Int): String = f"$streamsIdsPath$streamID%010d"

  def put(streamValue: streamService.StreamValue): streamService.StreamRecord = {
    val id = client.create()
      .creatingParentsIfNeeded()
      .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
      .forPath(streamsIdsPath, Array.emptyByteArray)

    val streamKey = StreamKey(getIDFromPath(id).toInt)

    val streamValueWithPath: streamService.StreamValue =
      streamService.StreamValue(
        streamValue.name,
        streamValue.partitions,
        streamValue.description,
        streamValue.ttl,
        Some(id)
      )

    val streamRecord =
      StreamRecord(streamKey, streamValueWithPath)

    client
      .setData()
      .forPath(id, streamRecord.toBinaryJson)

    streamRecord
  }


//  def exists(streamKey: streamService.StreamKey): Boolean = {
//    Option(client.checkExists().forPath(buildPath(streamKey.id)))
//      .exists(_ => true)
//  }

  def get(streamKey: streamService.StreamKey): Option[streamService.StreamRecord] = {
    val streamValueOpt =
      scala.util.Try(client.getData.forPath(buildPath(streamKey.id)))
    val streamRecord = streamValueOpt
      .toOption
      .map(data => StreamRecord.fromBinaryJson(data))

    streamRecord
  }

  //  def delete(streamKey: StreamKey, streamPath: StreamNamePath): Boolean = {
  //    if (checkExists(streamKey)) {
  //      val isDeleted = Option(client.getData.forPath(buildPath(streamKey.id)))
  //        .map(data => StreamRecord.fromBinaryJson(data))
  //        .map(streamRecord => streamPath.delete(streamRecord.name, this))
  //        .map(_ => client.setData().forPath(buildPath(streamKey.id), Array.emptyByteArray))
  //        .exists(_ => true)
  //
  //      if (isDeleted && logger.isDebugEnabled) {
  //        logger.debug(s"Stream with id ${streamKey.id} is ${if (isDeleted) "" else "not"} deleted")
  //      }
  //      isDeleted
  //    }
  //    else {
  //      false
  //    }
  //  }
}

