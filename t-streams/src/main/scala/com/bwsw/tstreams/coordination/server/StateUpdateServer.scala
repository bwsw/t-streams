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

package com.bwsw.tstreams.coordination.server

import java.net.{DatagramPacket, SocketAddress}

import com.bwsw.tstreams.agents.consumer.subscriber.{Subscriber, TransactionBufferWorker}
import com.bwsw.tstreams.common.UdpServer
import com.bwsw.tstreamstransactionserver.netty.Protocol
import com.bwsw.tstreamstransactionserver.rpc.TransactionState

import scala.collection.mutable

/**
  * Created by Ivan Kudryavtsev on 21.04.17.
  */
class StateUpdateServer(host: String, port: Int, threads: Int,
                        transactionsBufferWorkers: mutable.Map[Int, TransactionBufferWorker],
                        authenticationKey: String) extends UdpServer(host, port, threads) {

  private val partitionCache = mutable.Map[Int, TransactionBufferWorker]()

  transactionsBufferWorkers
    .foreach(id_w => id_w._2.getPartitions().foreach(p => partitionCache(p) = id_w._2))

  override def handleRequest(client: SocketAddress, reqAny: AnyRef): Unit = {
    val req = reqAny.asInstanceOf[TransactionState]

    if (Subscriber.logger.isDebugEnabled())
      Subscriber.logger.debug(s"Transaction State Update: $req")

    if (req.authKey == authenticationKey) {
      if (partitionCache.contains(req.partition))
        partitionCache(req.partition).updateTransactionState(req)
      else
        Subscriber.logger.warn(s"Unknown partition ${req.partition} found in Message: $req.")
    } else {
      Subscriber.logger.warn(s"Update req ($req) was received, but expected is $authenticationKey, message has been ignore.")
    }
  }

  override def getObjectFromDatagramPacket(packet: DatagramPacket): Option[AnyRef] =
    Some(Protocol.decode(packet.getData.take(packet.getLength), TransactionState))

  override def getKey(objAny: AnyRef): Int = objAny.asInstanceOf[TransactionState].partition
}
