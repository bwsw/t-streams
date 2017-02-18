package testutils

import com.bwsw.tstreamstransactionserver.netty.server.Server
import com.bwsw.tstreamstransactionserver.options.{ServerBuilder, StorageOptions, ZookeeperOptions}

/**
  * Created by ivan on 29.01.17.
  */
object TestStorageServer {

  private val serverBuilder = new ServerBuilder()
    .withZookeeperOptions(new ZookeeperOptions(endpoints = s"127.0.0.1:${TestUtils.ZOOKEEPER_PORT}"))

  def get(): Server = {
    val transactionServer = serverBuilder.withServerStorageOptions(new StorageOptions(path = TestUtils.getTmpDir())).build()
    transactionServer.start()
    transactionServer
  }

  def dispose(transactionServer: Server) = {
    transactionServer.shutdown()
  }

}
