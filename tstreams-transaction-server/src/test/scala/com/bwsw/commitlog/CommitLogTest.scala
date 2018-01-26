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
import java.util.concurrent.TimeUnit

import com.bwsw.commitlog.CommitLogFlushPolicy.{OnCountInterval, OnRotation, OnTimeInterval}
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.util.Random


class CommitLogTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll
    with TableDrivenPropertyChecks {

  private val directory = Paths.get("target", "clt").toString
  private val record = "sample record".getBytes
  private val recordSize = Integer.BYTES + CommitLogRecord.headerSize + record.length
  private val token = 54231651
  private val fileIDGen = CommitLogUtils.createIDGenerator
  private val writeFileInterval: Long = 50

  override def beforeAll(): Unit = new File(directory).mkdirs()

  "CommitLog" should "write correctly (OnRotation policy)" in {
    val flushIntervalSeconds = 1

    // interval to wait until commitLog wrote data into a file and open next file
    val waitingInterval = TimeUnit.SECONDS.toMillis(flushIntervalSeconds) + writeFileInterval

    val commitLog = new CommitLog(flushIntervalSeconds, directory, OnRotation, fileIDGen)
    val fileName1 = commitLog.putRec(record, 0, token)
    val file1 = new File(fileName1)
    file1.exists() shouldBe true
    file1.length() shouldBe 0
    Thread.sleep(waitingInterval)

    val fileName2_1 = commitLog.putRec(record, 0, token)
    file1.length() shouldBe recordSize
    val file2_1 = new File(fileName2_1)
    file2_1.exists() shouldBe true
    file2_1.length() shouldBe 0
    val fileName2_2 = commitLog.putRec(record, 0, token)
    file2_1.length() shouldBe 0

    val fileName3 = commitLog.putRec(record, 0, token, startNew = true)
    file2_1.length() shouldBe recordSize * 2
    val file3 = new File(fileName3)
    file3.exists() shouldBe true
    file3.length() shouldBe 0
    commitLog.close()
    file3.length() shouldBe recordSize

    fileName1 shouldNot be(fileName2_1)
    fileName2_1 shouldBe fileName2_2
    fileName2_1 shouldNot be(fileName3)
    fileName1 shouldNot be(fileName3)
  }

  it should "write correctly (OnTimeInterval policy) when startNewFileSeconds > policy flushIntervalSeconds" in {
    val flushPolicyInterval = 1
    val flushIntervalSeconds = flushPolicyInterval * 2

    // interval to wait until commitLog wrote data into a file and open next file
    val waitingInterval = TimeUnit.SECONDS.toMillis(flushPolicyInterval) + writeFileInterval
    val commitLog = new CommitLog(flushIntervalSeconds, directory, OnTimeInterval(flushPolicyInterval), fileIDGen)
    val fileName1_1 = commitLog.putRec(record, 0, token)
    val file1 = new File(fileName1_1)
    file1.exists() shouldBe true
    file1.length() shouldBe 0
    Thread.sleep(waitingInterval)
    file1.length() shouldBe 0
    val fileName1_2 = commitLog.putRec(record, 0, token)
    file1.length() shouldBe recordSize
    Thread.sleep(waitingInterval)
    file1.length() shouldBe recordSize

    val fileName2 = commitLog.putRec(record, 0, token)
    file1.length() shouldBe recordSize * 2
    val file2 = new File(fileName2)
    file2.exists() shouldBe true
    file2.length() shouldBe 0
    commitLog.close()
    file2.length() shouldBe recordSize
    val fileName3 = commitLog.putRec(record, 0, token)
    file2.length() shouldBe recordSize
    val file3 = new File(fileName3)
    file3.exists() shouldBe true
    file3.length() shouldBe 0
    Thread.sleep(waitingInterval)
    file3.length() shouldBe 0
    Thread.sleep(waitingInterval)
    val fileName4 = commitLog.putRec(record, 0, token)
    file3.length() shouldBe recordSize
    val file4 = new File(fileName4)
    file4.exists() shouldBe true
    file4.length() shouldBe 0
    val fileName5 = commitLog.putRec(record, 0, token, startNew = true)
    file4.length() shouldBe recordSize
    val file5 = new File(fileName5)
    file5.exists() shouldBe true
    file5.length() shouldBe 0
    commitLog.close()
    file5.length() shouldBe recordSize

    fileName1_1 shouldBe fileName1_2
    fileName1_1 shouldNot be(fileName2)
    fileName2 shouldNot be(fileName3)
    fileName3 shouldNot be(fileName4)
    fileName4 shouldNot be(fileName5)
  }

  it should "write correctly (OnTimeInterval policy) when startNewFileSeconds < policy flushIntervalSeconds" in {
    val flushIntervalSeconds = 1
    val flushPolicyInterval = flushIntervalSeconds * 2

    // interval to wait until commitLog wrote data into a file and open next file
    val waitingInterval = TimeUnit.SECONDS.toMillis(flushIntervalSeconds) + writeFileInterval

    val commitLog = new CommitLog(flushIntervalSeconds, directory, OnTimeInterval(flushPolicyInterval), fileIDGen)
    val fileName1_1 = commitLog.putRec(record, 0, token)
    val file1 = new File(fileName1_1)
    file1.exists() shouldBe true
    file1.length() shouldBe 0
    Thread.sleep(waitingInterval)
    val fileName2 = commitLog.putRec(record, 0, token)
    file1.length() shouldBe recordSize
    fileName1_1 shouldNot be(fileName2)
    val file2 = new File(fileName2)
    file2.exists() shouldBe true
    file2.length() shouldBe 0
    commitLog.close()
    file2.length() shouldBe recordSize
  }

  it should "write correctly (OnCountInterval policy)" in {
    val flushIntervalSeconds = 1

    // interval to wait until commitLog wrote data into a file and open next file
    val waitingInterval = TimeUnit.SECONDS.toMillis(flushIntervalSeconds) + writeFileInterval
    val commitLog = new CommitLog(flushIntervalSeconds, directory, OnCountInterval(2), fileIDGen)
    val fileName1_1 = commitLog.putRec(record, 0, token)
    val fileName1_2 = commitLog.putRec(record, 0, token)
    fileName1_1 shouldBe fileName1_2
    val file1 = new File(fileName1_1)
    file1.exists() shouldBe true
    file1.length() shouldBe 0
    val fileName1_3 = commitLog.putRec(record, 0, token)
    fileName1_1 shouldBe fileName1_3
    file1.exists() shouldBe true
    file1.length() shouldBe recordSize * 2
    Thread.sleep(waitingInterval)
    file1.length() shouldBe recordSize * 2
    val fileName2 = commitLog.putRec(record, 0, token)
    file1.length() shouldBe recordSize * 3
    fileName1_1 shouldNot be(fileName2)
    val file2 = new File(fileName2)
    file2.exists() shouldBe true
    file2.length() shouldBe 0
    commitLog.close()
    file2.length() shouldBe recordSize
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

  override def afterAll: Unit = {
    List(directory).foreach(dir =>
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
