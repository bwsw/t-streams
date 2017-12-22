package util

import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode

object SubscriberUtils {

  def putSubscriberInStream(client: CuratorFramework,
                            path: String,
                            partition: Int,
                            subscriber: String
                           ): Unit = {
    client.create()
      .creatingParentsIfNeeded()
      .withMode(CreateMode.PERSISTENT)
      .forPath(
        s"$path/subscribers/$partition/$subscriber",
        Array.emptyByteArray
      )
  }

  def deleteSubscriberInStream(client: CuratorFramework,
                               path: String,
                               partition: Int,
                               subscriber: String
                              ): Unit = {
    client.delete()
      .forPath(
        s"$path/subscribers/$partition/$subscriber"
      )
  }
}
