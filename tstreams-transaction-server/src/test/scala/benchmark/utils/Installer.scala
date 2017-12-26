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

package benchmark.utils

import java.io.File
import java.util.concurrent.{CountDownLatch, TimeUnit}

import benchmark.Options._
import org.apache.commons.io.FileUtils

import scala.concurrent.Await
import scala.concurrent.duration._


trait Installer {
  private val storageOptions = serverBuilder.getStorageOptions

  def clearDB() = {
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogRawDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogRocksDirectory))
  }

  def startTransactionServer() = {
    val latch = new CountDownLatch(1)
    new Thread(() =>
      serverBuilder
        .build()
        .start(latch.countDown())
    ).start()
    latch.await(5000, TimeUnit.MILLISECONDS)
  }

  def createStream(name: String, partitions: Int): Int = {
    val client = clientBuilder
      .build()


    val streamID = if (Await.result(client.checkStreamExists(name), 5.seconds)) {
      Await.result(client.getStream(name), 5.seconds).map(_.id).getOrElse(
        throw new IllegalArgumentException("Something wrong with stream")
      )
    } else {
      Await.result(client.putStream(name, partitions, None, 5), 5.seconds)
    }
    client.shutdown()
    streamID
  }

  def deleteStream(name: String) = {
    val client = clientBuilder.build()
    Await.result(client.delStream(name), 10.seconds)

    client.shutdown()
  }
}
