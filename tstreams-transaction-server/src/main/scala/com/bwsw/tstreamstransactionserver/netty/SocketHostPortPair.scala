
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

package com.bwsw.tstreamstransactionserver.netty

import org.apache.commons.validator.routines.{DomainValidator, InetAddressValidator}

import scala.util.Try

case class SocketHostPortPair(address: String, port: Int) {
  override def toString: String = s"$address:$port"

  def get = s"$address:$port"
}

object SocketHostPortPair {
  private val domainValidator = DomainValidator.getInstance()
  private val inetAddressValidator = InetAddressValidator.getInstance()

  def fromString(hostPortPair: String): Option[SocketHostPortPair] = {
    Option(hostPortPair.lastIndexOf(':'))
      .filter(_ != -1)
      .flatMap { splitIndex =>
        val (address, portAsString) =
          hostPortPair.splitAt(splitIndex)
        Try(portAsString.tail.toInt)
          .toOption
          .flatMap(port =>
            validateAndCreate(address, port)
          )
      }
  }

  def validateAndCreate(ipAddress: String, port: Int): Option[SocketHostPortPair] = {
    if (isValid(ipAddress, port))
      Some(SocketHostPortPair(ipAddress, port))
    else
      None
  }

  def isValid(inetAddress: String, port: Int): Boolean = {
    val isHostname = domainValidator.isValid(inetAddress)
    val isIPAddress = inetAddressValidator.isValid(inetAddress)

    val isPortOkay = port > 0 && port < 65536

    if (isPortOkay && (isIPAddress || isHostname))
      true
    else
      false
  }
}