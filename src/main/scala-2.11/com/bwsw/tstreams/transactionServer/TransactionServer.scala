package com.bwsw.tstreams.transactionServer

import com.twitter.finagle.Thrift
import com.twitter.server.TwitterServer
import com.twitter.util.Await

/**
  * Created by Ivan Kudryavtsev on 22.10.16.
  */
object TransactionServer extends TwitterServer {
  val service = new rpcImpl.TransactionServerServiceImpl()

  def main() {
    val server = Thrift.server.serveIface("localhost:8888", service)

    onExit {
      server.close()
    }

    Await.ready(server)
  }
}
