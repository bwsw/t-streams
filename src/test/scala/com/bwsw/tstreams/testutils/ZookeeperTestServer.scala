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

import java.util.Properties

import org.apache.zookeeper.server.quorum.QuorumPeerConfig
import org.apache.zookeeper.server.{ServerConfig, ZooKeeperServerMain}

/**
  * Created by Ivan Kudryavtsev on 18.02.17.
  */
class ZookeeperTestServer(zookeperPort: Int, tmp: String) {

  val properties = new Properties()
  properties.setProperty("tickTime", "2000")
  properties.setProperty("initLimit", "10")
  properties.setProperty("syncLimit", "5")
  properties.setProperty("dataDir", s"$tmp")
  properties.setProperty("clientPort", s"$zookeperPort")

  val zooKeeperServer = new ZooKeeperServerMain
  val quorumConfiguration = new QuorumPeerConfig()
  quorumConfiguration.parseProperties(properties)

  val configuration = new ServerConfig()

  configuration.readFrom(quorumConfiguration)

  new Thread() {
    override def run() = {
      zooKeeperServer.runFromConfig(configuration)
    }
  }.start()
}
