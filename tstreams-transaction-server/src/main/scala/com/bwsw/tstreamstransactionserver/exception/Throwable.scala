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
package com.bwsw.tstreamstransactionserver.exception

import java.net.SocketTimeoutException

object Throwable {
  val TokenInvalidExceptionMessage: String = "Token isn't valid."
  val serverConnectionExceptionMessage: String = "Can't connect to Server."
  val serverUnreachableExceptionMessage: String = "Server is unreachable."
  val zkGetMasterExceptionMessage: String = "Can't get master from ZooKeeper."
  val zkNoConnectionExceptionMessage: String = "Can't connect to ZooKeeper server(s): "
  val PackageTooBigExceptionMessagePart: String = "A size of client[/"

  def byText(text: String): Throwable = text match {
    case TokenInvalidExceptionMessage =>
      new TokenInvalidException
    case message if message.matches(s"Stream (.*) doesn't exist in database!") =>
      new StreamDoesNotExist(message, isPartialMessage = false)
    case message if message.contains(PackageTooBigExceptionMessagePart) =>
      new PackageTooBigException(text)
    case _ =>
      new Exception(text)
  }

  class TokenInvalidException(message: String = TokenInvalidExceptionMessage)
    extends IllegalArgumentException(message)

  class ServerConnectionException
    extends SocketTimeoutException(serverConnectionExceptionMessage)

  class ServerUnreachableException(socket: String)
    extends SocketTimeoutException(s"Server $socket is unreachable.")

  class RequestTimeoutException(reqId: Long, ttl: Long)
    extends Exception(s"Request $reqId exceeds $ttl ms.")

  class ZkGetMasterException(endpoints: String)
    extends Exception(s"Can't get master from ZooKeeper servers: $endpoints.")

  class ServerIsSlaveException
    extends IllegalStateException("Server role is slave now - no write operations can be done.")

  class ZkNoConnectionException(endpoints: String)
    extends Exception(new StringBuilder(zkNoConnectionExceptionMessage).append(endpoints).append('!').toString())

  class MethodDoesNotFoundException(method: String)
    extends IllegalArgumentException(new StringBuilder(method).append(" isn't implemented!").toString())

  class InvalidSocketAddress(message: String)
    extends IllegalArgumentException(message)

  class MasterIsPersistentZnodeException(path: String)
    extends IllegalArgumentException(s"Master node: $path is persistent node, but should be ephemeral.")

  class MasterDataIsIllegalException(path: String, data: String)
    extends IllegalArgumentException(s"Master node: $path, data is: $data. It's not ip adrress.")

  class MasterPathIsAbsent(path: String)
    extends IllegalArgumentException(s"Master path: $path doesn't exist.")

  class StreamDoesNotExist(stream: String, isPartialMessage: Boolean = true) extends {
    private val message: String = if (isPartialMessage) s"Stream $stream doesn't exist in database!" else stream
  } with NoSuchElementException(message)

  class PackageTooBigException(msg: String = "")
    extends Exception(msg)

  object ClientIllegalOperationAfterShutdown
    extends IllegalStateException("It's not allowed do any operations after client shutdown!")
}
