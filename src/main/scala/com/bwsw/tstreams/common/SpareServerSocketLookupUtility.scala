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

package com.bwsw.tstreams.common

import java.io.IOException
import java.net.{InetAddress, ServerSocket}

import scala.util.Random

/**
  * Created by Ivan Kudryavtsev on 05.09.16.
  */
object SpareServerSocketLookupUtility {

  private def checkIfAvailable(hostOrIp: String, port: Int): Boolean = {
    var ss: ServerSocket = null

    try {
      ss = new ServerSocket(port, 1, InetAddress.getByName(hostOrIp))
      ss.setReuseAddress(true)
      ss.close()
      return true
    } catch {
      case e: IOException =>
    } finally {
      if (ss != null) ss.close()
    }
    false
  }

  def findSparePort(hostOrIp: String, fromPort: Int, toPort: Int): Option[Int] = synchronized {
    Random.shuffle(fromPort to toPort).find(port => checkIfAvailable(hostOrIp, port))
  }
}
