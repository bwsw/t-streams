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

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 03.08.16.
  */
class LockUtilTests extends FlatSpec with Matchers with BeforeAndAfterAll {
  "After call lockOrDie" should "lock to be received" in {
    val l = new ReentrantLock()
    LockUtil.lockOrDie(l, (100, TimeUnit.MILLISECONDS))
    l.getHoldCount shouldBe 1
    l.unlock()
  }

  "After withLockOrDieDo" should "lock to be received and released" in {
    val l = new ReentrantLock()
    LockUtil.withLockOrDieDo[Unit](l, (100, TimeUnit.MILLISECONDS), None, () => {
      l.getHoldCount shouldBe 1
    })
    l.getHoldCount shouldBe 0
  }


  "After withLockOrDieDo" should "lock to be received and released even with exception" in {
    val l = new ReentrantLock()
    try {
      LockUtil.withLockOrDieDo[Unit](l, (100, TimeUnit.MILLISECONDS), None, () => {
        l.getHoldCount shouldBe 1
        throw new Exception("expected")
      })
    } catch {
      case e: Exception =>
        e.getMessage shouldBe "expected"
        l.getHoldCount shouldBe 0
    }
  }

}
