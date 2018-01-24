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

package com.bwsw.commitlog

import java.io.{File, IOException}
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes

import com.bwsw.commitlog.CommitLogFlushPolicy.{OnCountInterval, OnRotation, OnTimeInterval}
import commitlog.Util
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.util.Random


class CommitLogTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll
    with TableDrivenPropertyChecks {

  private val dir = Paths.get("target", "clt").toString
  private val record = "sample record".getBytes
  private val recordSize = Integer.BYTES + CommitLogRecord.headerSize + record.length
  private val fileIDGen = Util.createIDGenerator
  private val token = 54231651

  override def beforeAll(): Unit = {
    new File(dir).mkdirs()
  }

  it should "write correctly (OnRotation policy)" in {
    val cl = new CommitLog(1, dir, OnRotation, fileIDGen)
    val f1 = cl.putRec(record, 0, token)
    val fileF1 = new File(f1)
    fileF1.exists() shouldBe true
    fileF1.length() shouldBe 0
    Thread.sleep(1100)

    val f21 = cl.putRec(record, 0, token)
    fileF1.length() shouldBe recordSize * 1
    val fileF21 = new File(f21)
    fileF21.exists() shouldBe true
    fileF21.length() shouldBe recordSize * 0
    val f22 = cl.putRec(record, 0, token)
    fileF21.length() shouldBe recordSize * 0

    val f3 = cl.putRec(record, 0, token, startNew = true)
    fileF21.length() shouldBe recordSize * 2
    val fileF3 = new File(f3)
    fileF3.exists() shouldBe true
    fileF3.length() shouldBe recordSize * 0
    cl.close()
    fileF3.length() shouldBe recordSize * 1

    f1 == f21 shouldBe false
    f21 shouldBe f22
    f21 == f3 shouldBe false
    f1 == f3 shouldBe false
  }

  it should "write correctly (OnTimeInterval policy) when startNewFileSeconds > policy seconds" in {
    val cl = new CommitLog(4, dir, OnTimeInterval(2), fileIDGen)
    val f11 = cl.putRec(record, 0, token)
    val fileF1 = new File(f11)
    fileF1.exists() shouldBe true
    fileF1.length() shouldBe recordSize * 0
    Thread.sleep(2100)
    fileF1.length() shouldBe recordSize * 0
    val f12 = cl.putRec(record, 0, token)
    fileF1.length() shouldBe recordSize * 1
    Thread.sleep(2100)
    fileF1.length() shouldBe recordSize * 1

    val f2 = cl.putRec(record, 0, token)
    fileF1.length() shouldBe recordSize * 2
    val fileF2 = new File(f2)
    fileF2.exists() shouldBe true
    fileF2.length() shouldBe recordSize * 0
    cl.close()
    fileF2.length() shouldBe recordSize * 1
    val f3 = cl.putRec(record, 0, token)
    fileF2.length() shouldBe recordSize * 1
    val fileF3 = new File(f3)
    fileF3.exists() shouldBe true
    fileF3.length() shouldBe recordSize * 0
    Thread.sleep(2100)
    fileF3.length() shouldBe recordSize * 0
    Thread.sleep(2100)
    val f4 = cl.putRec(record, 0, token)
    fileF3.length() shouldBe recordSize * 1
    val fileF4 = new File(f4)
    fileF4.exists() shouldBe true
    fileF4.length() shouldBe recordSize * 0
    val f5 = cl.putRec(record, 0, token, startNew = true)
    fileF4.length() shouldBe recordSize * 1
    val fileF5 = new File(f5)
    fileF5.exists() shouldBe true
    fileF5.length() shouldBe recordSize * 0
    cl.close()
    fileF5.length() shouldBe recordSize * 1

    f11 shouldBe f12
    f11 == f2 shouldBe false
    f2 == f3 shouldBe false
    f3 == f4 shouldBe false
    f4 == f5 shouldBe false
  }

  it should "write correctly (OnTimeInterval policy) when startNewFileSeconds < policy seconds" in {
    val cl = new CommitLog(2, dir, OnTimeInterval(4), fileIDGen)
    val f11 = cl.putRec(record, 0, token)
    val fileF1 = new File(f11)
    fileF1.exists() shouldBe true
    fileF1.length() shouldBe recordSize * 0
    Thread.sleep(2100)
    val f2 = cl.putRec(record, 0, token)
    fileF1.length() shouldBe recordSize * 1
    f11 == f2 shouldBe false
    val fileF2 = new File(f2)
    fileF2.exists() shouldBe true
    fileF2.length() shouldBe recordSize * 0
    cl.close()
    fileF2.length() shouldBe recordSize * 1
  }

  it should "write correctly (OnCountInterval policy)" in {
    val cl = new CommitLog(2, dir, OnCountInterval(2), fileIDGen)
    val f11 = cl.putRec(record, 0, token)
    val f12 = cl.putRec(record, 0, token)
    f11 shouldBe f12
    val fileF1 = new File(f11)
    fileF1.exists() shouldBe true
    fileF1.length() shouldBe 0
    val f13 = cl.putRec(record, 0, token)
    f11 shouldBe f13
    fileF1.exists() shouldBe true
    fileF1.length() shouldBe recordSize * 2
    Thread.sleep(2100)
    fileF1.length() shouldBe recordSize * 2
    val f2 = cl.putRec(record, 0, token)
    fileF1.length() shouldBe recordSize * 3
    f11 == f2 shouldBe false
    val fileF2 = new File(f2)
    fileF2.exists() shouldBe true
    fileF2.length() shouldBe recordSize * 0
    cl.close()
    fileF2.length() shouldBe recordSize * 1
  }

  it should "convert Int to Array[Byte] correctly" in {
    for (_ <- 1 to 100) {
      val i = Random.nextInt()
      val bytes = CommitLog.intToBytes(i)

      CommitLog.bytesToInt(bytes) shouldBe i
    }

    forAll(Table(
      ("i", "bytes"),
      (111929672, Array(0x06, 0xab, 0xe9, 0x48)),
      (-71144835, Array(0xfb, 0xc2, 0x6a, 0x7d)),
      (1193751489, Array(0x47, 0x27, 0x33, 0xc1)),
      (-608806, Array(0xff, 0xf6, 0xb5, 0xda)),
      (-2072998014, Array(0x84, 0x70, 0x8f, 0x82)),
      (2126452693, Array(0x7e, 0xbf, 0x17, 0xd5)),
      (730360143, Array(0x2b, 0x88, 0x69, 0x4f)),
      (-1008452659, Array(0xc3, 0xe4, 0x3b, 0xcd)))) { (i, bytes) =>
      CommitLog.intToBytes(i) shouldEqual bytes.map(_.toByte)
      CommitLog.bytesToInt(bytes.map(_.toByte)) shouldBe i
    }
  }

  override def afterAll(): Unit = {
    List(dir).foreach(dir =>
      Files.walkFileTree(Paths.get(dir), new SimpleFileVisitor[Path]() {
        override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
          Files.delete(file)
          FileVisitResult.CONTINUE
        }

        override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
          Files.delete(dir)
          FileVisitResult.CONTINUE
        }
      }))
  }
}
