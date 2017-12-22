package util


import java.io.File

import com.bwsw.tstreamstransactionserver.netty.client.api.TTSClient
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.{SingleNodeServerBuilder, TestSingleNodeServer}
import org.apache.commons.io.FileUtils

class ZkSeverTxnServerTxnClient(val transactionServer: TestSingleNodeServer,
                                val client: TTSClient,
                                val serverBuilder: SingleNodeServerBuilder)
{

  def operate(operation: TestSingleNodeServer => Unit): Unit = {
    try {
      operation(transactionServer)
    }
    catch {
      case throwable: Throwable =>
        throw throwable
    }
    finally {
      closeDbsAndDeleteDirectories()
    }
  }

  def closeDbsAndDeleteDirectories(): Unit = {
    transactionServer.shutdown()
    client.shutdown()

    val storageOptions = serverBuilder.getStorageOptions

    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogRocksDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogRawDirectory))
    new File(storageOptions.path).delete()
  }
}
