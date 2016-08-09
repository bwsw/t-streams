package com.bwsw.tstreams.data.cassandra

import com.bwsw.tstreams.common.{CassandraConnectorConf, MetadataConnectionPool}


/**
  * Factory for creating cassandra storage instances
  */
class CassandraStorageFactory {

  /**
    *
    * @param conf Cassandra client options
    * @return Instance of CassandraStorage
    */
  def getInstance(conf: CassandraConnectorConf, keyspace: String): CassandraStorage =
    new CassandraStorage(MetadataConnectionPool.getCluster(conf), MetadataConnectionPool.getSession(conf, keyspace), keyspace)

}
