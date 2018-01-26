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

import java.io._
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}


class CommitLogFileTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  val dir = "target/clf"

  override def beforeAll(): Unit = new File(dir).mkdirs()

  "CommitLogFile" should "compute and check md5 correctly" in {
    val pathEmptyFile: String = "target/clf/4444/44/44/0.dat"
    val pathEmptyFileMD5: String = "target/clf/4444/44/44/0.md5"
    val pathNotEmptyFile: String = "target/clf/4444/44/44/1.dat"
    val pathNotEmptyFileMD5: String = "target/clf/4444/44/44/1.md5"
    val pathNotEmptyFile2: String = "target/clf/4444/44/44/2.dat"
    val pathNotEmptyFile2MD5: String = "target/clf/4444/44/44/2.md5"
    val pathNotEmptyFile3: String = "target/clf/4444/44/44/3.dat"
    val md5EmptyFile = "d41d8cd98f00b204e9800998ecf8427e".toUpperCase.getBytes
    val md5NotEmptyFile = "c4ca4238a0b923820dcc509a6f75849b".toUpperCase.getBytes
    new File("target/clf/4444/44/44").mkdirs()
    new File(pathEmptyFile).createNewFile()
    new File(pathNotEmptyFile).createNewFile()
    new File(pathNotEmptyFile2).createNewFile()
    new File(pathNotEmptyFile3).createNewFile()

    new FileOutputStream(pathEmptyFileMD5) {
      write(md5EmptyFile)
      close()
    }
    new FileOutputStream(pathNotEmptyFile) {
      write("1".getBytes())
      close()
    }
    new FileOutputStream(pathNotEmptyFileMD5) {
      write(md5NotEmptyFile)
      close()
    }
    new FileOutputStream(pathNotEmptyFile2) {
      write("2".getBytes())
      close()
    }
    new FileOutputStream(pathNotEmptyFile2MD5) {
      write(md5NotEmptyFile)
      close()
    }
    new FileOutputStream(pathNotEmptyFile3) {
      write("3".getBytes())
      close()
    }

    val clfEmpty = new CommitLogFile(pathEmptyFile)
    val clfNotEmpty = new CommitLogFile(pathNotEmptyFile)
    val clfNotEmpty2 = new CommitLogFile(pathNotEmptyFile2)
    val clfNotEmpty3 = new CommitLogFile(pathNotEmptyFile3)

    clfEmpty.calculateMD5 sameElements md5EmptyFile shouldBe true
    clfNotEmpty.calculateMD5 sameElements md5NotEmptyFile shouldBe true
    clfNotEmpty2.calculateMD5 sameElements md5NotEmptyFile shouldBe false
    clfEmpty.md5Exists() shouldBe true

    clfEmpty.checkMD5() shouldBe true
    clfNotEmpty.md5Exists() shouldBe true
    clfNotEmpty.checkMD5() shouldBe true
    clfNotEmpty2.md5Exists() shouldBe true
    clfNotEmpty2.checkMD5() shouldBe false
    clfNotEmpty3.md5Exists() shouldBe false
    intercept[FileNotFoundException] {
      clfNotEmpty3.checkMD5()
    }
    intercept[FileNotFoundException] {
      clfNotEmpty3.getMD5
    }
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
