package com.bwsw.tstreamstransactionserver.netty.server.storage.rocks

import java.util.concurrent.TimeUnit

import com.bwsw.tstreamstransactionserver.netty.server.db.KeyValueDbBatch
import com.bwsw.tstreamstransactionserver.netty.server.db.rocks.RocksDbDescriptor
import com.bwsw.tstreamstransactionserver.netty.server.storage.Storage
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.{RocksStorageOptions, StorageOptions}
import org.rocksdb.ColumnFamilyOptions

abstract class RocksStorage(storageOpts: StorageOptions,
                            rocksOpts: RocksStorageOptions,
                            readOnly: Boolean = false)
  extends Storage {

  protected val columnFamilyOptions: ColumnFamilyOptions =
    rocksOpts.createColumnFamilyOptions()

  protected val commonDescriptors =
    scala.collection.immutable.Seq(
      RocksDbDescriptor(Storage.lastOpenedTransactionStorageDescriptorInfo, columnFamilyOptions),
      RocksDbDescriptor(Storage.lastCheckpointedTransactionStorageDescriptorInfo, columnFamilyOptions),
      RocksDbDescriptor(Storage.consumerStoreDescriptorInfo, columnFamilyOptions),
      RocksDbDescriptor(Storage.transactionAllStoreDescriptorInfo, columnFamilyOptions,
        TimeUnit.MINUTES.toSeconds(rocksOpts.transactionExpungeDelayMin).toInt
      ),
      RocksDbDescriptor(Storage.transactionOpenStoreDescriptorInfo, columnFamilyOptions)
    )
}
