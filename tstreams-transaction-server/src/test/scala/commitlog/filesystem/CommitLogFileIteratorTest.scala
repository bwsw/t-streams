package commitlog.filesystem

import java.io.{BufferedOutputStream, File, FileOutputStream, IOException}
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.util.concurrent.atomic.AtomicLong

import com.bwsw.commitlog.CommitLog
import com.bwsw.commitlog.filesystem.CommitLogFileIterator
import commitlog.Util
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}


class CommitLogFileIteratorTest extends FlatSpec with Matchers with BeforeAndAfterAll {
  val dir = "target/clfi"
  private val fileIDGenerator = Util.createIDGenerator

  override def beforeAll() = {
    new File(dir).mkdirs()
  }

  it should "read record from file" in {
    val commitLog = new CommitLog(1, dir, iDGenerator = fileIDGenerator)
    val fileName = commitLog.putRec(Array[Byte](2, 3, 4), 1, startNew = false)
    commitLog.close()
    val commitLogFileIterator = new CommitLogFileIterator(fileName)
    if (commitLogFileIterator.hasNext()) {
      val record = commitLogFileIterator.next().right.get
      record.message sameElements Array[Byte](2, 3, 4).deep shouldBe true
      record.messageType shouldBe (1:Byte)
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
      record.messageType shouldBe (5:Byte)
    }
    commitLogFileIterator.hasNext shouldBe true
    if (commitLogFileIterator.hasNext()) {
      val record = commitLogFileIterator.next().right.get
      record.message sameElements Array[Byte](7, 8, 9).deep shouldBe true
      record.messageType shouldBe (6:Byte)
    }
    commitLogFileIterator.hasNext shouldBe true
    if (commitLogFileIterator.hasNext()) {
      val record = commitLogFileIterator.next().right.get
      record.message sameElements Array[Byte](2, 3, 4).deep shouldBe true
      record.messageType shouldBe (1:Byte)
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
      record.messageType shouldBe (5:Byte)
    }
    commitLogFileIterator.hasNext shouldBe true
    if (commitLogFileIterator.hasNext()) {
      val record = commitLogFileIterator.next().right.get
      record.message sameElements Array[Byte](7, 8, 9).deep shouldBe true
      record.messageType shouldBe (6:Byte)
    }
    commitLogFileIterator.hasNext shouldBe true
    if (commitLogFileIterator.hasNext()) {
      intercept[NoSuchElementException] {
        throw commitLogFileIterator.next().left.get
      }
    }
    commitLogFileIterator.hasNext shouldBe false
  }

  override def afterAll = {
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
