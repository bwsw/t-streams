package com.bwsw.tstreams.common

import java.net.InetSocketAddress

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by Ivan Kudryavtsev on 04.08.16.
  */
class NetworkUtilTests extends FlatSpec with Matchers with BeforeAndAfterAll {

  "Build a list of three hosts" should "return list of 3 items of type InetSocketAddress host" in {
    val l = NetworkUtil.getInetSocketAddressCompatibleHostList("h1:2821,h2:2821,h3:2821")
    l.size shouldBe 3
    l.foreach(h => {
      h.isInstanceOf[InetSocketAddress] shouldBe true
      h.getPort shouldBe 2821
    })
    l.head.getHostName shouldBe "h1"
    l.tail.head.getHostName shouldBe "h2"
    l.tail.tail.head.getHostName shouldBe "h3"
  }
}
