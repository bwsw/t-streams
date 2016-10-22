package com.bwsw.tstreams.transactionServer

/**
  * Created by Ivan Kudryavtsev on 22.10.16.
  */
class Authenticator(var isMasterAuthMode: Boolean, peerToken: String, clientToken: String) {

  def setMasterAuthMode(isMasterAuthMode: Boolean) = {
    this.isMasterAuthMode = isMasterAuthMode }

  def authClient(token: String): Boolean = {
    if(!isMasterAuthMode)
      false
    else
      token == clientToken
  }

  def authPeer(token: String): Boolean = {
    token == peerToken
  }
}
