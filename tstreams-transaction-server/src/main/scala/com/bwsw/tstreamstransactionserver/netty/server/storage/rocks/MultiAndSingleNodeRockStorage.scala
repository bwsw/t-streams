package com.bwsw.tstreamstransactionserver.netty.server.storage.rocks

import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDbManager
import com.bwsw.tstreamstransactionserver.netty.server.db.rocks.{RocksDbDescriptor, RocksDbManager}
import com.bwsw.tstreamstransactionserver.netty.server.storage.Storage
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.{RocksStorageOptions, StorageOptions}

final class MultiAndSingleNodeRockStorage(storageOpts: StorageOptions,
                                          rocksOpts: RocksStorageOptions,
                                          readOnly: Boolean = false)
  extends RocksStorage(
    storageOpts,
    rocksOpts,
    readOnly) {
  private val rocksMetaServiceDB: KeyValueDbManager =
    new RocksDbManager(
      storageOpts.path + java.io.File.separatorChar + storageOpts.metadataDirectory,
      rocksOpts,
      commonDescriptors ++ List(
        RocksDbDescriptor(Storage.commitLogStoreDescriptorInfo, columnFamilyOptions),
        RocksDbDescriptor(Storage.bookkeeperLogStoreDescriptorInfo, columnFamilyOptions)
      ),
      readOnly
    )

  override def getStorageManager: KeyValueDbManager = {
    rocksMetaServiceDB
  }
}
