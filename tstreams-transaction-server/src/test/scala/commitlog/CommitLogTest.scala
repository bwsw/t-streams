package commitlog

import java.io.{File, IOException}
import java.nio.file._
import java.nio.file.attribute.BasicFileAttributes
import java.util.concurrent.atomic.AtomicLong

import com.bwsw.commitlog.{CommitLog, CommitLogRecord}
import com.bwsw.commitlog.CommitLogFlushPolicy.{OnCountInterval, OnRotation, OnTimeInterval}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}



class CommitLogTest
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll {

  private val dir = new StringBuffer().append("target").append(File.separatorChar).append("clt").toString
  private val rec = "sample record".map(_.toByte).toArray
  private val recordSize = rec.length + CommitLogRecord.headerSize


  private val fileIDGen = Util.createIDGenerator

  override def beforeAll() = {
    new File(dir).mkdirs()
  }

  it should "write correctly (OnRotation policy)" in {
    val cl = new CommitLog(1, dir, OnRotation, fileIDGen)
    val f1 = cl.putRec(rec, 0)
    val fileF1 = new File(f1)
    fileF1.exists() shouldBe true
    fileF1.length() == 0 shouldBe true
    Thread.sleep(1100)

    val f21 = cl.putRec(rec, 0)
    fileF1.length() == recordSize*1 shouldBe true
    val fileF21 = new File(f21)
    fileF21.exists() shouldBe true
    fileF21.length() == recordSize*0 shouldBe true
    val f22 = cl.putRec(rec, 0)
    fileF21.length() == recordSize*0 shouldBe true

    val f3 = cl.putRec(rec, 0, true)
    fileF21.length() == recordSize*2 shouldBe true
    val fileF3 = new File(f3)
    fileF3.exists() shouldBe true
    fileF3.length() == recordSize*0 shouldBe true
    cl.close()
    fileF3.length() == recordSize*1 shouldBe true

    f1 == f21 shouldBe false
    f21 == f22 shouldBe true
    f21 == f3 shouldBe false
    f1 == f3 shouldBe false
  }

  it should "write correctly (OnTimeInterval policy) when startNewFileSeconds > policy seconds" in {
    val cl = new CommitLog(4, dir, OnTimeInterval(2), fileIDGen)
    val f11 = cl.putRec(rec, 0)
    val fileF1 = new File(f11)
    fileF1.exists() shouldBe true
    fileF1.length() == recordSize*0 shouldBe true
    Thread.sleep(2100)
    fileF1.length() == recordSize*0 shouldBe true
    val f12 = cl.putRec(rec, 0)
    fileF1.length() == recordSize*1 shouldBe true
    Thread.sleep(2100)
    fileF1.length() == recordSize*1 shouldBe true

    val f2 = cl.putRec(rec, 0)
    fileF1.length() == recordSize*2 shouldBe true
    val fileF2 = new File(f2)
    fileF2.exists() shouldBe true
    fileF2.length() == recordSize*0 shouldBe true
    cl.close()
    fileF2.length() == recordSize*1 shouldBe true
    val f3 = cl.putRec(rec, 0)
    fileF2.length() == recordSize*1 shouldBe true
    val fileF3 = new File(f3)
    fileF3.exists() shouldBe true
    fileF3.length() == recordSize*0 shouldBe true
    Thread.sleep(2100)
    fileF3.length() == recordSize*0 shouldBe true
    Thread.sleep(2100)
    val f4 = cl.putRec(rec, 0)
    fileF3.length() == recordSize*1 shouldBe true
    val fileF4 = new File(f4)
    fileF4.exists() shouldBe true
    fileF4.length() == recordSize*0 shouldBe true
    val f5 = cl.putRec(rec, 0, true)
    fileF4.length() == recordSize*1 shouldBe true
    val fileF5 = new File(f5)
    fileF5.exists() shouldBe true
    fileF5.length() == recordSize*0 shouldBe true
    cl.close()
    fileF5.length() == recordSize*1 shouldBe true

    f11 == f12 shouldBe true
    f11 == f2 shouldBe false
    f2 == f3 shouldBe false
    f3 == f4 shouldBe false
    f4 == f5 shouldBe false
  }

  it should "write correctly (OnTimeInterval policy) when startNewFileSeconds < policy seconds" in {
    val cl = new CommitLog(2, dir, OnTimeInterval(4), fileIDGen)
    val f11 = cl.putRec(rec, 0)
    val fileF1 = new File(f11)
    fileF1.exists() shouldBe true
    fileF1.length() == recordSize*0 shouldBe true
    Thread.sleep(2100)
    val f2 = cl.putRec(rec, 0)
    fileF1.length() == recordSize*1 shouldBe true
    f11 == f2 shouldBe false
    val fileF2 = new File(f2)
    fileF2.exists() shouldBe true
    fileF2.length() == recordSize*0 shouldBe true
    cl.close()
    fileF2.length() == recordSize*1 shouldBe true
  }

  it should "write correctly (OnCountInterval policy)" in {
    val cl = new CommitLog(2, dir, OnCountInterval(2), fileIDGen)
    val f11 = cl.putRec(rec, 0)
    val f12 = cl.putRec(rec, 0)
    f11 == f12 shouldBe true
    val fileF1 = new File(f11)
    fileF1.exists() shouldBe true
    fileF1.length() == 0 shouldBe true
    val f13 = cl.putRec(rec, 0)
    f11 == f13 shouldBe true
    fileF1.exists() shouldBe true
    fileF1.length() == recordSize*2 shouldBe true
    Thread.sleep(2100)
    fileF1.length() == recordSize*2 shouldBe true
    val f2 = cl.putRec(rec, 0)
    fileF1.length() == recordSize*3 shouldBe true
    f11 == f2 shouldBe false
    val fileF2 = new File(f2)
    fileF2.exists() shouldBe true
    fileF2.length() == recordSize*0 shouldBe true
    cl.close()
    fileF2.length() == recordSize*1 shouldBe true
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
