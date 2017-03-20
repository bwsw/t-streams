package com.bwsw.tstreamstransactionserver.options

import com.bwsw.tstreamstransactionserver.netty.server.Server
import ServerOptions._
import CommonOptions.ZookeeperOptions

class ServerBuilder private(authOpts: AuthOptions, zookeeperOpts: ZookeeperOptions, bootstrapOpts: BootstrapOptions,
                            storageOpts: StorageOptions, serverReplicationOpts: ServerReplicationOptions,
                            rocksStorageOpts: RocksStorageOptions, commitLogOpts: CommitLogOptions,
                            packageTransmissionOpts: PackageTransmissionOptions) {
  private val authOptions = authOpts
  private val zookeeperOptions = zookeeperOpts
  private val bootstrapOptions = bootstrapOpts
  private val storageOptions = storageOpts
  private val serverReplicationOptions = serverReplicationOpts
  private val rocksStorageOptions = rocksStorageOpts
  private val commitLogOptions = commitLogOpts
  private val packageTransmissionOptions = packageTransmissionOpts

  def this() = this(AuthOptions(), ZookeeperOptions(), BootstrapOptions(), StorageOptions(),
    ServerReplicationOptions(), RocksStorageOptions(), CommitLogOptions(), PackageTransmissionOptions())

  def withAuthOptions(authOptions: AuthOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, storageOptions, serverReplicationOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions)

  def withZookeeperOptions(zookeeperOptions: ZookeeperOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, storageOptions, serverReplicationOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions)

  def withBootstrapOptions(bootstrapOptions: BootstrapOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, storageOptions, serverReplicationOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions)

  def withServerStorageOptions(serverStorageOptions: StorageOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, serverStorageOptions, serverReplicationOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions)

  def withServerReplicationOptions(serverReplicationOptions: ServerReplicationOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, storageOptions, serverReplicationOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions)

  def withServerRocksStorageOptions(serverStorageRocksOptions: RocksStorageOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, storageOptions, serverReplicationOptions, serverStorageRocksOptions, commitLogOptions, packageTransmissionOptions)

  def withPackageTransmissionOptions(packageTransmissionOptions: PackageTransmissionOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, storageOptions, serverReplicationOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions)

  def withCommitLogOptions(commitLogOptions: CommitLogOptions) =
    new ServerBuilder(authOptions, zookeeperOptions, bootstrapOptions, storageOptions, serverReplicationOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions)

  def build() = new Server(authOptions, zookeeperOptions, bootstrapOptions,
    storageOptions, serverReplicationOptions, rocksStorageOptions, commitLogOptions, packageTransmissionOptions)

  def getZookeeperOptions() = zookeeperOptions.copy()

  def getAuthOptions() = authOptions.copy()

  def getBootstrapOptions() = bootstrapOptions.copy()

  def getStorageOptions() = storageOptions.copy()

  def getServerReplicationOptions() = serverReplicationOptions.copy()

  def getRocksStorageOptions() = rocksStorageOptions.copy()

  def getPackageTransmissionOptions() = packageTransmissionOptions.copy()

  def getCommitLogOptions() = commitLogOptions.copy()
}