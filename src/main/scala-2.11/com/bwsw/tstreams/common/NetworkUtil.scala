package com.bwsw.tstreams.common

import java.net.InetSocketAddress

import com.aerospike.client.Host

/**
  * Created by ivan on 04.08.16.
  */
object NetworkUtil {
  /**
    * transforms host:port,host:port to list(Host, Host) for Aerospike
    *
    * @param h
    * @return
    */
  def getAerospikeCompatibleHostList(h: String): List[Host] =
    h.split(',').map((sh: String) => new Host(sh.split(':').head, Integer.parseInt(sh.split(':').tail.head))).toList

  /**
    * transforms host:port,host:port to list(InetSocketAddr, InetSocketAddr) for C*
    *
    * @param h
    * @return
    */
  def getInetSocketAddressCompatibleHostList(h: String): List[InetSocketAddress] =
    h.split(',').map((sh: String) => new InetSocketAddress(sh.split(':').head, Integer.parseInt(sh.split(':').tail.head))).toList


}
