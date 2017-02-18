package testutils

import java.util.Properties

import org.apache.zookeeper.server.quorum.QuorumPeerConfig
import org.apache.zookeeper.server.{ServerConfig, ZooKeeperServerMain}

/**
  * Created by Ivan Kudryavtsev on 18.02.17.
  */
class ZookeeperTestServer(zookeperPort: Int, tmp: String) {

  val properties = new Properties()
  properties.setProperty("tickTime", "2000")
  properties.setProperty("initLimit", "10")
  properties.setProperty("syncLimit", "5")
  properties.setProperty("dataDir", s"$tmp")
  properties.setProperty("clientPort", s"$zookeperPort")

  val zooKeeperServer = new ZooKeeperServerMain
  val quorumConfiguration = new QuorumPeerConfig()
  quorumConfiguration.parseProperties(properties)

  val configuration = new ServerConfig()

  configuration.readFrom(quorumConfiguration)

  new Thread() {
    override def run() = {
      zooKeeperServer.runFromConfig(configuration)
    }
  }.start()
}
