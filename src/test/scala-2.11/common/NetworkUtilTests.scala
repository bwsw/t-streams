package common

import java.net.InetSocketAddress

import com.aerospike.client.Host
import com.bwsw.tstreams.common.NetworkUtil
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by ivan on 04.08.16.
  */
class NetworkUtilTests extends FlatSpec with Matchers with BeforeAndAfterAll  {
  "Build Aerospike compatible list of three hosts" should "return list of 3 items of type Aerospike host" in {
    val l = NetworkUtil.getAerospikeCompatibleHostList("h1:8080,h2:8080,h3:8080")
    l.size shouldBe 3
    l.foreach(h => {
      h.isInstanceOf[Host] shouldBe true
      h.port shouldBe 8080 })
    l.head.name shouldBe "h1"
    l.tail.head.name shouldBe "h2"
    l.tail.tail.head.name shouldBe "h3"
  }

  "Build C* compatible list of three hosts" should "return list of 3 items of type InetSocketAddress host" in {
    val l = NetworkUtil.getInetSocketAddressCompatibleHostList("h1:2821,h2:2821,h3:2821")
    l.size shouldBe 3
    l.foreach(h => {
      h.isInstanceOf[InetSocketAddress] shouldBe true
      h.getPort shouldBe 2821 })
    l.head.getHostName shouldBe "h1"
    l.tail.head.getHostName shouldBe "h2"
    l.tail.tail.head.getHostName shouldBe "h3"
  }
}
