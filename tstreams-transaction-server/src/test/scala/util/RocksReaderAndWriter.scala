package util


import java.io.File

import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDbBatch
import com.bwsw.tstreamstransactionserver.netty.server.db.zk.ZookeeperStreamRepository
import com.bwsw.tstreamstransactionserver.netty.server.storage.rocks.MultiAndSingleNodeRockStorage
import com.bwsw.tstreamstransactionserver.netty.server.streamService.StreamService
import com.bwsw.tstreamstransactionserver.netty.server.transactionDataService.TransactionDataService
import com.bwsw.tstreamstransactionserver.netty.server.{RocksReader, RocksWriter}
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.{RocksStorageOptions, StorageOptions}
import org.apache.commons.io.FileUtils
import org.apache.curator.framework.CuratorFramework



class RocksReaderAndWriter(zkClient: CuratorFramework,
                           val storageOptions: StorageOptions,
                           rocksStorageOpts: RocksStorageOptions)
{

  private val rocksStorage =
    new MultiAndSingleNodeRockStorage(
      storageOptions,
      rocksStorageOpts
    )

  private val streamRepository =
    new ZookeeperStreamRepository(zkClient, s"${storageOptions.streamZookeeperDirectory}")

  private val transactionDataService =
    new TransactionDataService(
      storageOptions,
      rocksStorageOpts,
      streamRepository
    )

  val rocksWriter = new RocksWriter(
    rocksStorage,
    transactionDataService
  )

  val rocksReader = new RocksReader(
    rocksStorage,
    transactionDataService
  )

  val streamService = new StreamService(
    streamRepository
  )

  def newBatch: KeyValueDbBatch =
    rocksWriter.getNewBatch

  def closeDBAndDeleteFolder(): Unit = {
    rocksStorage.getStorageManager.closeDatabases()
    transactionDataService.closeTransactionDataDatabases()

    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.metadataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.dataDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogRocksDirectory))
    FileUtils.deleteDirectory(new File(storageOptions.path + java.io.File.separatorChar + storageOptions.commitLogRawDirectory))
    new File(storageOptions.path).delete()
  }
}
