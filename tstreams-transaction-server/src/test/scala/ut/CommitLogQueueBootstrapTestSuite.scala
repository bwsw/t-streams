package ut

import java.util.concurrent.PriorityBlockingQueue

import com.bwsw.commitlog.filesystem.{CommitLogCatalogue, CommitLogStorage}
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.CommitLogQueueBootstrap
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import util.Utils.startZkServerAndGetIt

import scala.collection.mutable.ArrayBuffer

class CommitLogQueueBootstrapTestSuite
  extends FlatSpec
    with Matchers
    with BeforeAndAfterAll {
  //arrange


  private lazy val (zkServer, zkClient) =
    startZkServerAndGetIt


  private lazy val bundle = util.Utils.getTransactionServerBundle(zkClient)
  private lazy val transactionServer = bundle.transactionServer

  private lazy val storageOptions = bundle.storageOptions


  private lazy val commitLogCatalogue =
    new CommitLogCatalogue(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogRawDirectory)
  private lazy val commitLogQueueBootstrap =
    new CommitLogQueueBootstrap(10, commitLogCatalogue, bundle.signleNodeCommitLogService)


  override def beforeAll(): Unit = {
    zkServer
    zkClient
  }


  "fillQueue" should "return an empty queue if there are no commit log files in a storage directory" in {
    //act
    val emptyQueue = commitLogQueueBootstrap.fillQueue()

    //assert
    emptyQueue shouldBe empty
  }

  "fillQueue" should "return a queue of a size that equals to a number of commit log files are in a storage directory" in {
    //arrange
    val numberOfFiles = 10
    createCommitLogFiles(numberOfFiles)

    //act
    val nonemptyQueue = commitLogQueueBootstrap.fillQueue()

    //assert
    nonemptyQueue should have size numberOfFiles
  }

  "fillQueue" should "return a queue with the time ordered commit log files" in {
    //arrange
    val numberOfFiles = 1
    createCommitLogFiles(numberOfFiles)
    createCommitLogFiles(numberOfFiles)
    createCommitLogFiles(numberOfFiles)

    //act
    val orderedQueue = commitLogQueueBootstrap.fillQueue()
    val orderedFiles = getOrderedFiles(orderedQueue)

    //assert
    orderedFiles shouldBe sorted
  }

  override def afterAll() = {
    zkClient.close()
    zkServer.close()
    bundle.closeDbsAndDeleteDirectories()
  }

  private def createCommitLogFiles(number: Int) = {
    val commitLogCatalogueByDate = new CommitLogCatalogue(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogRawDirectory)

    (0 until number).foreach(fileNamePrefix => {
      commitLogCatalogueByDate.createFile(fileNamePrefix.toString)
    })
  }

  private def getOrderedFiles(orderedQueue: PriorityBlockingQueue[CommitLogStorage]): ArrayBuffer[CommitLogStorage] = {
    val orderedFiles = ArrayBuffer[CommitLogStorage]()
    var path = orderedQueue.poll()
    while (path != null) {
      orderedFiles += path
      path = orderedQueue.poll()
    }

    orderedFiles
  }
}
