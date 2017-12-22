package benchmark

import com.bwsw.tstreamstransactionserver.netty.client.{ClientBuilder, InetClientProxy}
import com.bwsw.tstreamstransactionserver.netty.server.singleNode.SingleNodeServerBuilder
import com.bwsw.tstreamstransactionserver.options.ClientOptions.{AuthOptions, ConnectionOptions}
import com.bwsw.tstreamstransactionserver.options.CommonOptions.ZookeeperOptions
import com.bwsw.tstreamstransactionserver.options.SingleNodeServerOptions.{AuthenticationOptions, CommitLogOptions}

private[benchmark] object Options {
  private val key = "pingstation"

  val clientAuthOption =
    AuthOptions(key = key)

  val clientConnectionOption =
    ConnectionOptions(requestTimeoutMs = 200)

  val sharedZookkeeperOption =
    ZookeeperOptions(endpoints = "127.0.0.1:37001,127.0.0.1:37002,127.0.0.1:37003")


  val clientBuilder: ClientBuilder = new ClientBuilder()
    .withAuthOptions(clientAuthOption)
    .withConnectionOptions(clientConnectionOption)
    .withZookeeperOptions(sharedZookkeeperOption)

  val serverAuthenticationOption =
    AuthenticationOptions(key = key)

  val serverBuilder: SingleNodeServerBuilder = new SingleNodeServerBuilder()
    .withAuthenticationOptions(serverAuthenticationOption)
    .withCommitLogOptions(CommitLogOptions(closeDelayMs = 1000))

  def inetClient = {
    new InetClientProxy(
      clientConnectionOption,
      clientAuthOption,
      sharedZookkeeperOption
    )
  }
}
