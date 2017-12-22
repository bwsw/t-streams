package ut

import com.bwsw.tstreamstransactionserver.netty.server.streamService.{StreamKey, StreamValue}
import org.scalatest.{FlatSpec, Matchers}

class StreamRecordValueServiceTest
  extends FlatSpec
    with Matchers
{

  "Key" should "be serialized/deserialized" in {
    val key = StreamKey(1)
    StreamKey.fromByteArray(key.toByteArray) shouldBe key
  }

  it should "be serialized/deserialized with negative transaction" in {
    val key = StreamKey(-1)
    StreamKey.fromByteArray(key.toByteArray) shouldBe key
  }

  "StreamWithoutKey" should "be serialized/deserialized without description" in {
    val stream = StreamValue("streamNumber1", 10, None, Long.MaxValue, None)
    StreamValue.fromByteArray(stream.toByteArray) shouldBe stream
  }

  it should "be serialized/deserialized with negative partitions" in {
    val stream = StreamValue("streamNumber1", -5, None, Long.MaxValue, None)
    StreamValue.fromByteArray(stream.toByteArray) shouldBe stream
  }

  it should "be serialized/deserialized with negative ttl" in {
    val stream = StreamValue("streamNumber1", -5, None, Long.MinValue, None)
    StreamValue.fromByteArray(stream.toByteArray) shouldBe stream
  }

  it should "be serialized/deserialized with description" in {
    val stream = StreamValue("streamNumber1", 70, Some("test"), Long.MaxValue, None)
    StreamValue.fromByteArray(stream.toByteArray) shouldBe stream
  }

  it should "be serialized/deserialized with empty description" in {
    val stream = StreamValue("streamNumber1", 16, Some(""), Long.MaxValue, None)
    StreamValue.fromByteArray(stream.toByteArray) shouldBe stream
  }
}
