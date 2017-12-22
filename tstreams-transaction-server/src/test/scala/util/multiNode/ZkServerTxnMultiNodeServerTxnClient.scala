package util.multiNode

import java.io.File

import com.bwsw.tstreamstransactionserver.netty.client.api.TTSClient
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.{CommonCheckpointGroupServerBuilder, TestCommonCheckpointGroupServer}
import org.apache.commons.io.FileUtils

class ZkServerTxnMultiNodeServerTxnClient(val transactionServer: TestCommonCheckpointGroupServer,
                                          val client: TTSClient,
                                          val serverBuilder: CommonCheckpointGroupServerBuilder) {
  def operate(operation: TestCommonCheckpointGroupServer => Unit): Unit = {
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

