package com.bwsw.tstreams.velocity

import com.bwsw.tstreams.common.CassandraHelper
import com.datastax.driver.core.Cluster

object MetadataCreator {
  def main(args: Array[String]) {
    import Common._
    val cluster = Cluster.builder().addContactPoint("localhost").build()
    val session = cluster.connect()
    try {
      session.execute(s"DROP KEYSPACE $keyspace")
    }
    catch {
      case e: Exception => println(s"msg=${e.getMessage}")
    }
    CassandraHelper.createKeyspace(session, keyspace)
    CassandraHelper.createMetadataTables(session, keyspace)
    cluster.close()
    session.close()
  }
}
