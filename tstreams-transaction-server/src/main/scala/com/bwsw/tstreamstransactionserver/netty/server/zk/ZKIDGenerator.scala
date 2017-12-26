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

package com.bwsw.tstreamstransactionserver.netty.server.zk

import com.bwsw.commitlog.IDGenerator
import org.apache.curator.RetryPolicy
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong

import scala.annotation.tailrec

final class ZKIDGenerator(curatorClient: CuratorFramework,
                          retryPolicy: RetryPolicy,
                          path: String)
  extends IDGenerator[Long] {

  private val distributedAtomicLong =
    new DistributedAtomicLong(
      curatorClient,
      path,
      retryPolicy
    )

  distributedAtomicLong.initialize(-1L)

  override def nextID: Long = {
    val operation = distributedAtomicLong.increment()
    if (operation.succeeded()) {
      val newID = operation.postValue()
      newID
    }
    else
      throw new Exception(
        s"Can't increment counter by 1: " +
          s"previous was ${operation.preValue()} " +
          s"but now it's ${operation.postValue()}."
      )
  }

  @tailrec
  override def currentID: Long = {
    val operation = distributedAtomicLong.get()
    if (operation.succeeded()) {
      operation.postValue()
    } else {
      currentID
    }
  }

  def setID(id: Long): Long = {
    distributedAtomicLong
      .trySet(id)
      .postValue()
  }
}
