package com.bwsw.tstreamstransactionserver.netty.client.zk

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.state.ConnectionState

class ZKConnectionLostListener(connection: CuratorFramework,
                               onConnectionStateChangeDo: ConnectionState => Unit) {
  private val listener = {
    val listener =
      ZKClient.addConnectionListener(
        connection,
        onConnectionStateChangeDo
      )
    listener
  }

  def stop(): Unit = {
    ZKClient
      .removeConnectionListener(
        connection,
        listener
      )
  }
}
