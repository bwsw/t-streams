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

package util


import java.net.{DatagramPacket, DatagramSocket, InetAddress}

final class UdpServer {
  private val port = Utils.getRandomPort
  private val socket: DatagramSocket = {
    new DatagramSocket(port)
  }

  def receive(timeout: Int): Array[Byte] = {
    if (timeout <= 0)
      socket.setSoTimeout(0)
    else
      socket.setSoTimeout(timeout)

    val receiveData: Array[Byte] = new Array[Byte](1024)
    val receivePacket = new DatagramPacket(receiveData, receiveData.length)
    socket.receive(receivePacket)

    receiveData.take(receivePacket.getLength)
  }

  def getPort: Int = port

  def getAddress: InetAddress = socket.getLocalAddress

  def getSocketAddress: String = s"${getAddress.getHostAddress}:$getPort"

  def close(): Unit = socket.close()
}
