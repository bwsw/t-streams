package com.bwsw.tstreams.common

import java.net.InetSocketAddress

/**
  * Created by Ivan Kudryavtsev on 04.08.16.
  */
object NetworkUtil {
  /**
    * transforms host:port,host:port to list(InetSocketAddr, InetSocketAddr) for C*
    *
    * @param h
    * @return
    */
  def getInetSocketAddressCompatibleHostList(h: String): List[InetSocketAddress] =
    h.split(',').map((sh: String) => new InetSocketAddress(sh.split(':').head, Integer.parseInt(sh.split(':').tail.head))).toList
}
