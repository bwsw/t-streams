package com.bwsw.tstreams.data.cassandra

import com.bwsw.tstreams.common.{CassandraConnectorConf, MetadataConnectionPool}


/**
  * Factory for creating cassandra storage instances
  */
class Factory {

  /**
    *
    * @param conf Cassandra client options
    * @return Instance of CassandraStorage
    */
  def getInstance(conf: CassandraConnectorConf, keyspace: String): Storage =
    new Storage(MetadataConnectionPool.getCluster(conf), MetadataConnectionPool.getSession(conf, keyspace), keyspace)

}
