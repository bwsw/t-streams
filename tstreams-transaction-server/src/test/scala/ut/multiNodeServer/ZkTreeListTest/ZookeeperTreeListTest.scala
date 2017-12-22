package ut.multiNodeServer.ZkTreeListTest

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.bookkeperService.hierarchy.LongZookeeperTreeList
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import util.Utils

class ZookeeperTreeListTest
  extends FlatSpec
    with BeforeAndAfterAll
    with Matchers
{

  private def uuid = java.util.UUID.randomUUID.toString

  private lazy val (zkServer, zkClient) = Utils.startZkServerAndGetIt

  override def afterAll(): Unit = {
    zkClient.close()
    zkServer.close()
  }


  "ZookeeperTreeListLong" should "return first entry id and last entry id as Nones" in {
    val treeListLong = new LongZookeeperTreeList(zkClient, s"/$uuid")

    treeListLong.lastEntityID shouldBe None
    treeListLong.firstEntityID shouldBe None
  }

  it should "return first entry id and last entry id the same as only one entity id was persisted" in {
    val treeListLong = new LongZookeeperTreeList(zkClient, s"/$uuid")

    val value = 1L
    treeListLong.createNode(value)

    treeListLong.firstEntityID shouldBe defined
    treeListLong.firstEntityID.get == value

    treeListLong.firstEntityID shouldBe treeListLong.lastEntityID
  }

  it should "return first entry id and last entry id properly" in {
    val treeListLong = new LongZookeeperTreeList(zkClient, s"/$uuid")

    val startNumber = 0
    val maxNumbers  = 10

    (startNumber to maxNumbers)
      .foreach(number => treeListLong.createNode(number))

    treeListLong.firstEntityID shouldBe Some(startNumber)
    treeListLong.lastEntityID  shouldBe Some(maxNumbers)
  }

  it should "return a next node of some node properly" in {
    val treeListLong = new LongZookeeperTreeList(zkClient, s"/$uuid")

    val startNumber = 0
    val maxNumbers  = 10

    (startNumber to maxNumbers)
      .foreach(number => treeListLong.createNode(number))

    val id = scala.util.Random.nextInt(maxNumbers)
    treeListLong.getNextNode(id) shouldBe Some(id + 1)
  }

  it should "not return a next node of last node" in {
    val treeListLong = new LongZookeeperTreeList(zkClient, s"/$uuid")

    val startNumber = 0
    val maxNumbers  = 10

    (startNumber to maxNumbers)
      .foreach(number => treeListLong.createNode(number))

    val id = maxNumbers
    treeListLong.getNextNode(id) shouldBe None
  }

  it should "return a previous node of some node correctly" in {
    val treeListLong = new LongZookeeperTreeList(zkClient, s"/$uuid")

    val startNumber = 0
    val maxNumbers  = 10

    (startNumber to maxNumbers)
      .foreach(number => treeListLong.createNode(number))

    val id = scala.util.Random.nextInt(maxNumbers + 1)
    treeListLong.getPreviousNode(id) shouldBe Some(id - 1)
  }

  it should "not return a previous node that doesn't exit" in {
    val treeListLong = new LongZookeeperTreeList(zkClient, s"/$uuid")

    val startNumber = 0
    val maxNumbers  = 10

    (startNumber to maxNumbers)
      .foreach(number => treeListLong.createNode(number))

    val id = -1
    treeListLong.getPreviousNode(id) shouldBe None
  }

  it should "delete the one node tree list correctly" in {
    val treeListLong = new LongZookeeperTreeList(zkClient, s"/$uuid")

    val startNumber = 0
    val maxNumbers  = 0

    val ids = (startNumber to maxNumbers).toArray

    ids.foreach(id =>
      treeListLong.createNode(id)
    )

    val id = 0
    treeListLong.deleteNode(id) shouldBe true

    treeListLong.firstEntityID shouldBe None
    treeListLong.lastEntityID  shouldBe None
  }

  it should "delete the first node correctly" in {
    val treeListLong = new LongZookeeperTreeList(zkClient, s"/$uuid")

    val startNumber = 0
    val maxNumbers  = 7

    val ids = (startNumber to maxNumbers).toArray

    ids.foreach(id =>
      treeListLong.createNode(id)
    )

    val firstID = startNumber
    val nextID  = startNumber + 1
    treeListLong.deleteNode(firstID) shouldBe true

    treeListLong.firstEntityID shouldBe Some(nextID)
    treeListLong.lastEntityID  shouldBe Some(maxNumbers)
  }

  it should "delete the last node correctly" in {
    val treeListLong = new LongZookeeperTreeList(zkClient, s"/$uuid")

    val startNumber = 0
    val maxNumbers  = 7

    val ids = (startNumber to maxNumbers).toArray

    ids.foreach(id =>
      treeListLong.createNode(id)
    )

    val lastID = maxNumbers
    val previousID = lastID - 1
    treeListLong.deleteNode(lastID) shouldBe true
    treeListLong.getNextNode(previousID) shouldBe None

    treeListLong.firstEntityID shouldBe Some(startNumber)
    treeListLong.lastEntityID  shouldBe Some(previousID)
  }


  it should "delete a node between first entity id and last entity id" in {
    val treeListLong = new LongZookeeperTreeList(zkClient, s"/$uuid")

    val startNumber = 0
    val maxNumbers  = 10
    val moveRangeToLeftBoundNumber = 2

    val ids = (startNumber to maxNumbers).toArray

    ids.foreach(id =>
      treeListLong.createNode(id)
    )

    val id = scala.util.Random.nextInt(
      maxNumbers - moveRangeToLeftBoundNumber
    ) + moveRangeToLeftBoundNumber

    val previousID = id - 1
    val nextID = id + 1
    treeListLong.deleteNode(id) shouldBe true
    treeListLong.getNextNode(previousID - 1) shouldBe Some(previousID)
    treeListLong.getNextNode(previousID) shouldBe Some(nextID)

    treeListLong.firstEntityID shouldBe Some(startNumber)
    treeListLong.lastEntityID  shouldBe Some(maxNumbers)
  }

  it should "delete nodes from [head, n], n - some positive number" in {
    val treeListLong = new LongZookeeperTreeList(zkClient, s"/$uuid")

    val startNumber = 0
    val maxNumbers  = 30

    val ids = (startNumber to maxNumbers).toArray

    ids.foreach(id =>
      treeListLong.createNode(id)
    )

    val number = scala.util.Random.nextInt(maxNumbers)
    treeListLong.deleteLeftNodes(number)

    treeListLong.firstEntityID shouldBe Some(number)
    treeListLong.lastEntityID  shouldBe Some(maxNumbers)
  }

  it should "delete nodes from [head, n], n - some positive number, which is greater than number of nodes list contains" in {
    val treeListLong = new LongZookeeperTreeList(zkClient, s"/$uuid")

    val startNumber = 0
    val maxNumbers  = 15

    val ids = (startNumber to maxNumbers).toArray

    ids.foreach(id =>
      treeListLong.createNode(id)
    )

    val number = maxNumbers + 1
    treeListLong.deleteLeftNodes(number)

    treeListLong.firstEntityID shouldBe None
    treeListLong.lastEntityID  shouldBe None
  }
}
