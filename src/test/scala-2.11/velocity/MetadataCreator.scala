package velocity

import com.datastax.driver.core.Cluster
import testutils.CassandraHelper

object MetadataCreator {
  def main(args: Array[String]) {
    import Common._
    val cluster = Cluster.builder().addContactPoint("localhost").build()
    val session = cluster.connect()
    CassandraHelper.createKeyspace(session, keyspace)
    CassandraHelper.createMetadataTables(session, keyspace)
  }
}
