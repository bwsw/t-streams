package com.bwsw.tstreams.common

import com.bwsw.tstreamstransactionserver.options.{AuthOptions, ClientBuilder, ClientOptions, ZookeeperOptions}


/**
  *
  * @param clientOptions
  * @param authOptions
  * @param zookeeperOptions
  */
class StorageClient(clientOptions: ClientOptions, authOptions: AuthOptions, zookeeperOptions: ZookeeperOptions) {
  private val map = scala.collection.mutable.Map[String,String]()
  private val clientBuilder = new ClientBuilder()

  val client = clientBuilder.withClientOptions(clientOptions).withAuthOptions(authOptions).withZookeeperOptions(zookeeperOptions).build()
}
