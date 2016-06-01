//package entities
//
//import com.bwsw.tstreams.entities.{TransactionSettings, CommitEntity}
//import com.datastax.driver.core.Cluster
//import com.datastax.driver.core.utils.UUIDs
//import org.scalatest.{BeforeAndAfterAll, Matchers, FlatSpec}
//import testutils.{CassandraHelper, RandomStringCreator}
//import scala.collection.mutable
//
//
//class CommitEntityTest extends FlatSpec with Matchers with BeforeAndAfterAll{
//  def randomString: String = RandomStringCreator.randomAlphaString(10)
//  val randomKeyspace = randomString
//  val temporaryCluster = Cluster.builder().addContactPoint("localhost").build()
//  val temporarySession = temporaryCluster.connect()
//  CassandraHelper.createKeyspace(temporarySession, randomKeyspace)
//  CassandraHelper.createMetadataTables(temporarySession, randomKeyspace)
//  val connectedSession = temporaryCluster.connect(randomKeyspace)
//
//  "CommitEntity.commit() CommitEntity.getTransactionAmount()" should
//    "commit - add new commit in commit metadata table and it should expire after some time(ttl param)," +
//    "getTransactionAmount - retrieve this transaction amount from commit_log" in {
//
//    val commitEntity = new CommitEntity("commit_log", connectedSession)
//    val stream = randomString
//    val txn = UUIDs.timeBased()
//    val partition = 10
//    val totalCnt = 123
//    val ttl = 3
//
//    commitEntity.commit(stream, partition, txn, totalCnt, ttl)
//
//    //read all transactions from oldest offset
//    val amount: (Int,Int) = commitEntity.getTransactionAmount(stream, partition, txn).get
//    var checkVal = true
//
//    checkVal &= amount._1 == totalCnt
//    Thread.sleep(3000)
//    val emptyQueue: mutable.Queue[TransactionSettings] = commitEntity.getTransactionsIterator(stream, partition, UUIDs.timeBased(), 1)
//    checkVal &= emptyQueue.isEmpty
//
//    checkVal shouldEqual true
//  }
//
//  "CommitEntity.commit() CommitEntity.getTransactions()" should
//    "commit - add new commit in commit metadata table" +
//      "getTransactions - retrieve this transactions from commit_log" in {
//
//    val commitEntity = new CommitEntity("commit_log", connectedSession)
//    val stream = randomString
//    val txn1 = UUIDs.timeBased()
//    val txn2 = UUIDs.timeBased()
//    val partition = 10
//    val totalCnt = 123
//    val ttl = 3
//
//    commitEntity.commit(stream, partition, txn1, totalCnt, ttl)
//    commitEntity.commit(stream, partition, txn2, totalCnt, ttl)
//
//    var checkVal = true
//
//    val queue = commitEntity.getTransactionsIterator(stream, partition, UUIDs.startOf(0), 2)
//    checkVal &= queue.size == 2
//    val txnSettings1 = queue.dequeue()
//    checkVal &= txnSettings1.txnUuid == txn1 && txnSettings1.totalItems == totalCnt
//    val txnSettings2 = queue.dequeue()
//    checkVal &= txnSettings2.txnUuid == txn2 && txnSettings2.totalItems == totalCnt
//
//    checkVal shouldEqual true
//  }
//
//  override def afterAll(): Unit = {
//    temporarySession.execute(s"DROP KEYSPACE $randomKeyspace")
//    connectedSession.close()
//    temporarySession.close()
//    temporaryCluster.close()
//  }
//}
