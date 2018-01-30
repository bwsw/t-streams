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

import java.net._
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Executors}

import com.google.common.util.concurrent.ThreadFactoryBuilder

import scala.collection.JavaConverters._
import scala.util.Try

/**
  * Created by Ivan Kudryavtsev on 20.04.17.
  */
abstract class UdpServer(host: String, port: Int, threads: Int) extends UdpProcessor {
  protected val executors = new Array[ExecutorService](threads)
  protected val keyExecutorMapping = new ConcurrentHashMap[Int, Int]()
  private val partitionCounter = new AtomicInteger(0)

  private val keyCounterMap = new ConcurrentHashMap[Int /* key */ , AtomicLong /* counter */ ]()
  private val executorCounterMap = new ConcurrentHashMap[Int /* execNo */ , AtomicLong /* counter */ ]()
  private val executorTaskTimeMap = new ConcurrentHashMap[Int /* execNo */ , AtomicLong /* counter */ ]()

  protected def assignPartitionExecutor(partition: Int): Int = {
    partitionCounter.getAndIncrement() % threads
  }

  executors.indices.foreach(idx => {
    executors(idx) = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(s"UdpServer-$idx").build())
    executorCounterMap.put(idx, new AtomicLong(0))
    executorTaskTimeMap.put(idx, new AtomicLong(0))
  })

  override def socketInitializer() = new DatagramSocket(null)

  override def bind(): Unit = {
    socket.bind(new InetSocketAddress(InetAddress.getByName(host), port))
  }

  def handleRequest(client: SocketAddress, req: AnyRef)

  def getObjectFromDatagramPacket(packet: DatagramPacket): Option[AnyRef]

  def getKey(objAny: AnyRef): Int

  def respond(client: SocketAddress, response: Array[Byte]): Unit = {
    socket.send(new DatagramPacket(response, response.size, client))
  }

  override def handleMessage(socket: DatagramSocket, packet: DatagramPacket): Unit = {

    Try(getObjectFromDatagramPacket(packet)).toOption.flatten.foreach(obj => {
      val objKey = getKey(obj)
      keyCounterMap.putIfAbsent(objKey, new AtomicLong(0))

      val execNoOpt = Option(keyExecutorMapping.getOrDefault(objKey, -1))
        .map(execNo => {
          if (execNo == -1) {
            val numberOfExecutor = assignPartitionExecutor(objKey)
            keyExecutorMapping.put(objKey, numberOfExecutor)

            numberOfExecutor
          } else {
            execNo
          }
        })

      val task = new Runnable {
        override def run(): Unit = {
          try {
            val begin = System.nanoTime()
            handleRequest(packet.getSocketAddress, obj)
            val end = System.nanoTime()
            executorTaskTimeMap.get(execNoOpt.get).addAndGet(end - begin)
          } catch {
            case e: SocketException => if (!socket.isClosed) throw e
          }
        }
      }

      executorCounterMap.get(execNoOpt.get).incrementAndGet()
      keyCounterMap.get(objKey).incrementAndGet()

      execNoOpt.foreach(execNo => executors(execNo).execute(task))
    })
  }

  override def start(): UdpServer = super.start().asInstanceOf[UdpServer]

  override def stop(): Unit = {
    super.stop()
    executors.foreach(_.shutdown())

    // dump counters
    for (k <- executorCounterMap.keys().asScala) {
      if (executorCounterMap.get(k).get() > 0) {
        logger.info(s"Executor $k processed ${executorCounterMap.get(k).get()} messages. " +
          s" Total time spent ${executorTaskTimeMap.get(k).get() / 1000000} ms, avg per query " +
          s"${executorTaskTimeMap.get(k).get() * 1.0f / executorCounterMap.get(k).get() / 1000000}")
      }
    }

    for (k <- keyCounterMap.keys().asScala) {
      if (keyCounterMap.get(k).get() > 0)
        logger.info(s"Key $k (Executor ${keyExecutorMapping.get(k)}) received ${keyCounterMap.get(k).get()} messages.")
    }
  }

}
