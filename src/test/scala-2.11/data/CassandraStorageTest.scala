package data

import java.net.InetSocketAddress
import java.util.UUID

import com.bwsw.tstreams.common.CassandraConnectionPool
import com.bwsw.tstreams.data.cassandra.CassandraStorage
import com.datastax.driver.core.utils.UUIDs
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.TestUtils

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.language.postfixOps


class CassandraStorageTest extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils {

  logger.info("Random keyspace: " + randomKeyspace)
  val sessionWithKeyspace = CassandraConnectionPool.getSession(List(new InetSocketAddress("localhost", 9042)), randomKeyspace)

  "CassandraStorage.init(), CassandraStorage.truncate() and CassandraStorage.remove()" should "create, truncate and remove data table" in {
    val cassandraStorage = new CassandraStorage(
      cluster = cluster,
      session = sessionWithKeyspace,
      keyspace = randomKeyspace)

    var checkVal = true

    try {
      //here we already have created tables for entitiesRepository
      cassandraStorage.remove()
      cassandraStorage.init()
      cassandraStorage.truncate()
    }
    catch {
      case e: Exception =>
        checkVal = false
        logger.info(e.toString)
    }

    checkVal shouldEqual true
  }

  "CassandraStorage.put() CassandraStorage.get()" should "insert data in cassandra storage and retrieve it" in {
    val cassandraStorage = new CassandraStorage(
      cluster = cluster,
      session = sessionWithKeyspace,
      keyspace = randomKeyspace)

    val streamName: String = "stream_name"
    val partition: Int = 0
    val transaction: UUID = UUIDs.timeBased()
    val data = "some_data"
    val cnt = 1000

    val jobs = ListBuffer[() => Unit]()

    for (i <- 0 until 1000) {
      val future = cassandraStorage.put(streamName, partition, transaction, 60 * 60 * 24, data.getBytes, i)
      jobs += future
    }

    jobs.foreach(x => x())

    val queue: mutable.Queue[Array[Byte]] = cassandraStorage.get(streamName, partition, transaction, 0, cnt - 1)
    val emptyQueueForLeftBound = cassandraStorage.get(streamName, partition, transaction, -100, -1)
    val emptyQueueForRightBound = cassandraStorage.get(streamName, partition, transaction, cnt, cnt + 100)

    var checkVal = true

    if (emptyQueueForLeftBound.nonEmpty || emptyQueueForRightBound.nonEmpty)
      checkVal = false

    while (queue.nonEmpty) {
      val part: String = new String(queue.dequeue())
      if (part != data)
        checkVal = false
    }

    checkVal shouldEqual true
  }


  override def afterAll(): Unit = {
    onAfterAll()
  }
}