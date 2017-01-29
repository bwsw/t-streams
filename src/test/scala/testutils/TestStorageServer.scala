package testutils

import com.bwsw.tstreamstransactionserver.configProperties.{ConfigMap, ServerConfig}
import com.bwsw.tstreamstransactionserver.netty.server.Server

import scala.collection.mutable

/**
  * Created by ivan on 29.01.17.
  */
object TestStorageServer {
  private def serverConfig(zookeeperEndpoints: String): ConfigMap = {
    val map = mutable.Map[String,String]()

    map += (("auth.token.active.max", "100"))
    map += (("auth.token.time.expiration", "120"))

    map += (("transaction.data.ttl.add", "50"))
    map += (("transaction.metadata.ttl.add", "50"))
    map += (("transaction.data.clean.amount", "200"))
    map += (("transaction.timeout.clean.opened(sec)", "10"))

    map += (("db.path", "/tmp"))
    map += (("db.path.transaction_meta", "transaction_meta"))
    map += (("db.path.transaction_data", "transaction_data"))
    map += (("db.path.stream", "stream"))

    map += (("je.evictor.maxThreads", "2"))
    map += (("je.maxMemory", "600000"))
    map += (("je.evictor.coreThreads", "2"))

    map += (("transactionServer.replication.endpoints", "127.0.0.1:46000"))
    map += (("transactionServer.replication.timeout.masterElection", "5000"))
    map += (("transactionServer.berkeleyReadPool", "2"))
    map += (("transactionServer.rocksDBWritePool", "4"))
    map += (("transactionServer.replication.name", "TestServer1"))
    map += (("transactionServer.replication.group", "testgroup"))
    map += (("transactionServer.listen", "127.0.0.1"))
    map += (("transactionServer.port", "46000"))
    map += (("transactionServer.rocksDBReadPool", "2"))
    map += (("transactionServer.pool", "4"))

    map += (("zk.endpoints", zookeeperEndpoints))
    map += (("zk.prefix", "/stream"))
    map += (("zk.timeout.betweenRetries", "500"))
    map += (("zk.timeout.connection", "10000"))
    map += (("zk.retries.max", "5"))
    map += (("zk.timeout.session", "10000"))

    map += (("rocksdb.create_if_missing", "true"))
    map += (("rocksdb.allow_os_buffer", "true"))
    map += (("rocksdb.compression", "lz4"))
    map += (("rocksdb.max_background_compactions", "1"))
    map += (("rocksdb.use_fsync", "true"))

    new ConfigMap(map.toMap)
  }

  var transactionServer: Server = null

  def startTransactionServer(zookeeperEndpoints: String) = {
    transactionServer = new Server(new ServerConfig(serverConfig(zookeeperEndpoints)))
    transactionServer.start()
  }

  def stopTransactionServer() = {
    transactionServer.close() //TODO: fix name
  }


}
