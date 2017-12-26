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

package it.packageTooBig

import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.exception.Throwable.PackageTooBigException
import com.bwsw.tstreamstransactionserver.netty.client.ClientBuilder
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.SingleNodeServerBuilder
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.TransportOptions
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import util.Utils
import util.Utils.startZkServerAndGetIt

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class ClientPackageTooBigTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll {

  private val packageTransmissionOptions =
    TransportOptions(maxMetadataPackageSize = 1000000)

  private lazy val serverBuilder = new SingleNodeServerBuilder()
    .withPackageTransmissionOptions(packageTransmissionOptions)

  private lazy val clientBuilder = new ClientBuilder()

  private lazy val (zkServer, zkClient) =
    startZkServerAndGetIt

  private val secondsToWait = 10

  override def beforeAll(): Unit = {
    zkServer
    zkClient
  }

  override def afterAll(): Unit = {
    zkClient.close()
    zkServer.close()
  }

  "Client" should "not allow to transmit amount of data that is greater than maxMetadataPackageSize or maxDataPackageSize (throw PackageTooBigException)" in {
    val bundle = Utils.startTransactionServerAndClient(
      zkClient, serverBuilder, clientBuilder
    )

    bundle.operate { _ =>
      val client = bundle.client
      assertThrows[PackageTooBigException] {
        Await.result(client.putTransactionData(
          1,
          1,
          1L,
          Array.fill(2)(new Array[Byte](packageTransmissionOptions.maxMetadataPackageSize)),
          1
        ), Duration(secondsToWait, TimeUnit.SECONDS))
      }

    }
  }
}
