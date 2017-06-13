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

package com.bwsw.tstreams.streams

import com.bwsw.tstreams.env.defaults.TStreamsFactoryStreamDefaults
import com.bwsw.tstreams.storage.StorageClient
import org.apache.curator.framework.CuratorFramework

/**
  * @param client          Client to storage
  * @param id              stream identifier
  * @param name            Name of the stream
  * @param partitionsCount Number of stream partitions
  * @param ttl             Time of transaction time expiration in seconds
  * @param path            Zk path of the stream
  * @param description     Some additional info about stream
  */
class Stream(val client: StorageClient,
             val curator: CuratorFramework,
             val id: Int,
             val name: String,
             val partitionsCount: Int,
             val ttl: Long,
             val path: String,
             val description: String) {
  if (ttl < TStreamsFactoryStreamDefaults.Stream.ttlSec.min)
    throw new IllegalArgumentException(s"The TTL must be greater or equal than ${TStreamsFactoryStreamDefaults.Stream.ttlSec.min} seconds.")

  def shutdown() = {
    client.shutdown()
  }
}
