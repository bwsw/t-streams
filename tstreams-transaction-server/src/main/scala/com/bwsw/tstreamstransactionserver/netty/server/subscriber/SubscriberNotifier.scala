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
package com.bwsw.tstreamstransactionserver.netty.server.subscriber

import java.net.{DatagramSocket, InetAddress}

import com.bwsw.tstreamstransactionserver.protocol.TransactionState
import org.slf4j.{Logger, LoggerFactory}

private object SubscriberNotifier {
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
}


private[server] class SubscriberNotifier {
  private val clientSocket =
    new DatagramSocket()
  @volatile private var isStopped = false

  def broadcast(subscribers: java.util.Collection[String],
                message: TransactionState): Unit = {
    if (subscribers.isEmpty || isStopped) {
      //do nothing
    } else {
      val binaryMessage = message.toByteArray

      if (SubscriberNotifier.logger.isDebugEnabled())
        SubscriberNotifier.logger.debug(
          s"Subscribers to broadcast: $subscribers"
        )

      subscribers.forEach(address => {
        val addressPort = address.split(":")
        val host = InetAddress.getByName(addressPort(0))
        val port = addressPort(1).toInt

        val sendPacket =
          new java.net.DatagramPacket(
            binaryMessage,
            binaryMessage.length,
            host,
            port
          )

        var isSent = false
        while (!isSent) {
          try {
            clientSocket.send(sendPacket)
            isSent = true
          } catch {
            case e: Exception =>
              SubscriberNotifier.logger
                .warn(s"Send $message to $host:$port failed. Exception is: $e")
          }
        }
      })
    }
  }

  def close(): Unit = {
    if (isStopped) {
      throw new IllegalStateException(
        "Socket is closed"
      )
    } else {
      isStopped = true
      clientSocket.close()
    }
  }

}
