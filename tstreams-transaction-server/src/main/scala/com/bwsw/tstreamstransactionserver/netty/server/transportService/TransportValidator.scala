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

package com.bwsw.tstreamstransactionserver.netty.server.transportService

import com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException
import com.bwsw.tstreamstransactionserver.netty.RequestMessage
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.TransportOptions

final class TransportValidator(packageTransmissionOpts: TransportOptions) {
  lazy val packageTooBigException = new PackageTooBigException(s"A size of client request is greater " +
    s"than maxMetadataPackageSize (${packageTransmissionOpts.maxMetadataPackageSize}) " +
    s"or maxDataPackageSize (${packageTransmissionOpts.maxDataPackageSize}).")

  val maxMetadataPackageSize: Int =
    packageTransmissionOpts.maxMetadataPackageSize

  val maxDataPackageSize: Int =
    packageTransmissionOpts.maxDataPackageSize

  def isTooBigMetadataMessage(message: RequestMessage): Boolean = {
    message.body.length > maxMetadataPackageSize
  }

  def isTooBigDataMessage(message: RequestMessage): Boolean = {
    message.body.length > maxDataPackageSize
  }
}
