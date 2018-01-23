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

import java.io.File

import com.bwsw.tstreamstransactionserver.netty.client.api.TTSClient
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.{SingleNodeServerBuilder, TestSingleNodeServer}
import org.apache.commons.io.FileUtils
import util.multiNode.Util

class ZkSeverTxnServerTxnClients(val transactionServer: TestSingleNodeServer,
                                 val clients: Array[TTSClient],
                                 val serverBuilder: SingleNodeServerBuilder) {

  def operate(operation: TestSingleNodeServer => Unit): Unit = {
    try {
      operation(transactionServer)
    }
    catch {
      case throwable: Throwable => throw throwable
    }
    finally {
      closeDbsAndDeleteDirectories()
    }
  }

  def closeDbsAndDeleteDirectories(): Unit = {
    transactionServer.shutdown()
    clients.foreach(client => client.shutdown())

    val storageOptions = serverBuilder.getStorageOptions
    Util.deleteDirectories(storageOptions)
  }
}
