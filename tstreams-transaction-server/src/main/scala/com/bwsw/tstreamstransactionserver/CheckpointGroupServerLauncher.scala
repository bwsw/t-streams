package com.bwsw.tstreamstransactionserver

import com.bwsw.tstreamstransactionserver.netty.server.multiNode.cg.CheckpointGroupServerBuilder
import com.bwsw.tstreamstransactionserver.options.loader.PropertyFileLoader
import com.bwsw.tstreamstransactionserver.options.loader.PropertyFileReader._
import com.bwsw.tstreamstransactionserver.options.{CommonOptions, SingleNodeServerOptions}

object CheckpointGroupServerLauncher
  extends App {

  val propertyFileLoader =
    PropertyFileLoader()

  val serverAuthOptions: SingleNodeServerOptions.AuthenticationOptions =
    loadServerAuthenticationOptions(propertyFileLoader)
  val zookeeperOptions: CommonOptions.ZookeeperOptions =
    loadZookeeperOptions(propertyFileLoader)
  val bootstrapOptions: SingleNodeServerOptions.BootstrapOptions =
    loadBootstrapOptions(propertyFileLoader)
  val checkpointGroupRoleOptions: SingleNodeServerOptions.CheckpointGroupRoleOptions =
    loadCheckpointGroupRoleOptions(propertyFileLoader)
  val serverStorageOptions: SingleNodeServerOptions.StorageOptions =
    loadServerStorageOptions(propertyFileLoader)
  val serverRocksStorageOptions: SingleNodeServerOptions.RocksStorageOptions =
    loadServerRocksStorageOptions(propertyFileLoader)
  val packageTransmissionOptions: SingleNodeServerOptions.TransportOptions =
    loadPackageTransmissionOptions(propertyFileLoader)
  val bookkeeperOptions =
    loadBookkeeperOptions(propertyFileLoader)
  val tracingOptions: CommonOptions.TracingOptions = loadTracingOptions(propertyFileLoader)

  val builder =
    new CheckpointGroupServerBuilder()

  val server = builder
    .withBootstrapOptions(bootstrapOptions)
    .withAuthenticationOptions(serverAuthOptions)
    .withCheckpointGroupRoleOptions(checkpointGroupRoleOptions)
    .withServerStorageOptions(serverStorageOptions)
    .withServerRocksStorageOptions(serverRocksStorageOptions)
    .withZookeeperOptions(zookeeperOptions)
    .withPackageTransmissionOptions(packageTransmissionOptions)
    .withBookkeeperOptions(bookkeeperOptions)
    .withTracingOptions(tracingOptions)
    .build()

  server.start()

}
