package util.multiNode

import java.io.File

import com.bwsw.tstreamstransactionserver.netty.server.{RocksReader, RocksWriter, TransactionServer}
import com.bwsw.tstreamstransactionserver.netty.server.multiNode.commitLogService.CommitLogService
import com.bwsw.tstreamstransactionserver.netty.server.storage.Storage
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataService
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions
import org.apache.commons.io.FileUtils

class MultiNodeBundle(val transactionServer: TransactionServer,
                      val rocksWriter: RocksWriter,
                      val rocksReader: RocksReader,
                      val multiNodeCommitLogService: CommitLogService,
                      storage: Storage,
                      transactionDataService: TransactionDataService,
                      val storageOptions: SingleNodeServerOptions.StorageOptions,
                      rocksOptions: SingleNodeServerOptions.RocksStorageOptions)
{
  def operate(operation: TransactionServer => Unit): Unit = {
    try {
      operation(transactionServer)
    }
    catch {
      case throwable: Throwable => throw throwable
    }
    finally {
      closeDbsAndDeleteDirectories()
    }
  }

  def closeDbsAndDeleteDirectories(): Unit = {
    storage.getStorageManager.closeDatabases()
    transactionDataService.closeTransactionDataDatabases()
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogRocksDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogRawDirectory))
    new File(storageOptions.path).delete()
  }
}

