package mastervoting

import java.util.UUID

import com.datastax.driver.core.Cluster
import testutils.TestUtils

import scala.collection.mutable.ListBuffer

object Validator extends TestUtils{
  def main(args: Array[String]) {
    if (args.isEmpty)
      throw new IllegalArgumentException("specify keyspace")
    val keyspace = args(0)

    val cluster = Cluster.builder().addContactPoint("localhost").build()
    val session = cluster.connect()

    val set = session.execute(s"select * from $keyspace.commit_log").all()
    val it = set.iterator()
    val buf = new ListBuffer[UUID]()
    while(it.hasNext){
      val row = it.next()
      buf += row.getUUID("transaction")
    }

    if (isSorted(buf))
      println("sorted")
    else
      println("not sorted")

    cluster.close()
    session.close()
  }
}
