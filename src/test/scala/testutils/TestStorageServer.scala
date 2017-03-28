package testutils

import java.util.concurrent.CountDownLatch

import com.bwsw.tstreamstransactionserver.netty.server.Server
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.ServerOptions.{CommitLogOptions, StorageOptions}
import com.bwsw.tstreamstransactionserver.options.{ServerBuilder}

/**
  * Created by Ivan Kudryavtsev on 29.01.17.
  */
object TestStorageServer {

  private val serverBuilder = new ServerBuilder()
    .withZookeeperOptions(new ZookeeperOptions(endpoints = s"127.0.0.1:${TestUtils.ZOOKEEPER_PORT}"))

  def get(): Server = {
    val transactionServer = serverBuilder
      .withServerStorageOptions(new StorageOptions(path = TestUtils.getTmpDir()))
      .withCommitLogOptions(new CommitLogOptions(commitLogCloseDelayMs = 100, commitLogToBerkeleyDBTaskDelayMs = 100))
      .build()
    val l = new CountDownLatch(1)
    new Thread(() => {
      l.countDown()
      transactionServer.start()
    }).start()
    l.await()
    transactionServer
  }

  def dispose(transactionServer: Server) = {
    transactionServer.shutdown()
  }

}
