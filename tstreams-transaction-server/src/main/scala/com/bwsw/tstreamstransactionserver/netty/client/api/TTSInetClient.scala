package com.bwsw.tstreamstransactionserver.netty.client.api

import org.apache.curator.framework.state.ConnectionState

trait TTSInetClient
  extends TTSClient {

  protected def onZKConnectionStateChanged(newState: ConnectionState): Unit = {}

  protected def onServerConnectionLost(): Unit = {}

  protected def onRequestTimeout(): Unit = {}
}
