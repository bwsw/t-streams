package util

import com.bwsw.tstreamstransactionserver.netty.server.singleNode.SingleNodeServer
import org.apache.curator.test.TestingServer

final class ZkSeverAndTransactionServer(val zkServer: TestingServer,
                                        val transactionServer: SingleNodeServer
                                 )
{
  def close(): Unit = {
    transactionServer.shutdown()
    zkServer.close()
  }
}

object ZkSeverAndTransactionServer{
  def apply(zkServer: TestingServer, transactionServer: SingleNodeServer): ZkSeverAndTransactionServer =
    new ZkSeverAndTransactionServer(zkServer, transactionServer)
}
