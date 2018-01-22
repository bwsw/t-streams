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

package com.bwsw.commitlog.filesystem

import java.io.{BufferedOutputStream, File, FileOutputStream, IOException}
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes

import com.bwsw.commitlog.{CommitLog, Util}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}


class CommitLogFileIteratorTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  val dir = "target/clfi"
  private val fileIDGenerator = Util.createIDGenerator

  override def beforeAll(): Unit = new File(dir).mkdirs()

  "CommitLogFileIterator" should "read record from file" in {
    val commitLog = new CommitLog(1, dir, iDGenerator = fileIDGenerator)
    val fileName = commitLog.putRec(Array[Byte](2, 3, 4), 1, startNew = false)
    commitLog.close()
    val commitLogFileIterator = new CommitLogFileIterator(fileName)
    if (commitLogFileIterator.hasNext()) {
      val record = commitLogFileIterator.next().right.get
      record.message sameElements Array[Byte](2, 3, 4).deep shouldBe true
      record.messageType shouldBe (1: Byte)
    }
    commitLogFileIterator.hasNext shouldBe false
  }

  it should "read several records from file correctly" in {
    val commitLog = new CommitLog(10, dir, iDGenerator = fileIDGenerator)
    commitLog.putRec(Array[Byte](6, 7, 8), 5, startNew = false)
    commitLog.putRec(Array[Byte](7, 8, 9), 6, startNew = false)
    val fileName = commitLog.putRec(Array[Byte](2, 3, 4), 1, startNew = false)
    commitLog.close()
    val commitLogFileIterator = new CommitLogFileIterator(fileName)
    commitLogFileIterator.hasNext shouldBe true
    if (commitLogFileIterator.hasNext()) {
      val record = commitLogFileIterator.next().right.get
      record.message sameElements Array[Byte](6, 7, 8).deep shouldBe true
      record.messageType shouldBe (5: Byte)
    }
    commitLogFileIterator.hasNext shouldBe true
    if (commitLogFileIterator.hasNext()) {
      val record = commitLogFileIterator.next().right.get
      record.message sameElements Array[Byte](7, 8, 9).deep shouldBe true
      record.messageType shouldBe (6: Byte)
    }
    commitLogFileIterator.hasNext shouldBe true
    if (commitLogFileIterator.hasNext()) {
      val record = commitLogFileIterator.next().right.get
      record.message sameElements Array[Byte](2, 3, 4).deep shouldBe true
      record.messageType shouldBe (1: Byte)
    }
    commitLogFileIterator.hasNext shouldBe false
  }

  it should "read as much records from corrupted file as it can" in {
    val commitLog = new CommitLog(10, dir, iDGenerator = fileIDGenerator)
    commitLog.putRec(Array[Byte](6, 7, 8), 5, startNew = false)
    commitLog.putRec(Array[Byte](7, 8, 9), 6, startNew = false)
    val fileName = commitLog.putRec(Array[Byte](2, 3, 4), 1, startNew = false)
    commitLog.close()

    val bytesArray: Array[Byte] = Files.readAllBytes(Paths.get(fileName))

    val croppedFileName = fileName + ".cropped"
    val outputStream = new BufferedOutputStream(new FileOutputStream(croppedFileName))
    outputStream.write(bytesArray.slice(0, 49))
    outputStream.close()

    val commitLogFileIterator = new CommitLogFileIterator(croppedFileName)
    commitLogFileIterator.hasNext shouldBe true
    if (commitLogFileIterator.hasNext()) {
      val record = commitLogFileIterator.next().right.get
      record.message sameElements Array[Byte](6, 7, 8).deep shouldBe true
      record.messageType shouldBe (5: Byte)
    }
    commitLogFileIterator.hasNext shouldBe true
    if (commitLogFileIterator.hasNext()) {
      val record = commitLogFileIterator.next().right.get
      record.message sameElements Array[Byte](7, 8, 9).deep shouldBe true
      record.messageType shouldBe (6: Byte)
    }
    commitLogFileIterator.hasNext shouldBe true
    if (commitLogFileIterator.hasNext()) {
      intercept[NoSuchElementException] {
        throw commitLogFileIterator.next().left.get
      }
    }
    commitLogFileIterator.hasNext shouldBe false
  }

  override def afterAll: Unit = {
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
