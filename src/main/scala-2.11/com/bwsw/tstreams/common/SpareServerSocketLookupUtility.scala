package com.bwsw.tstreams.common

/**
  * Created by ivan on 05.09.16.
  */
object SpareServerSocketLookupUtility {

  private def checkIfAvailable(hostOrIp: String, port: Int): Boolean = ???

  def findSparePort(hostOrIp: String, fromPort: Int, toPort: Int): Option[Int] = synchronized {
    (fromPort to toPort).find(port => checkIfAvailable(hostOrIp, port))
  }
}
