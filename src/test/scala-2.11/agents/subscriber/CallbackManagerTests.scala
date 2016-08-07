package agents.subscriber

import com.bwsw.tstreams.coordination.pubsub.messages.{ProducerTopicMessage, ProducerTransactionStatus}
import com.bwsw.tstreams.coordination.pubsub.subscriber.CallbackManager
import com.datastax.driver.core.utils.UUIDs
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import org.scalamock.scalatest.MockFactory

/**
  * Created by ivan on 06.08.16.
  */
class CallbackManagerTests  extends FlatSpec with Matchers with MockFactory {
  val cm = new CallbackManager

  "Ensure initialization" should "be correct" in {
    cm.getCount() shouldBe 0
  }

  "Ensure callback" should "added" in {
    val m = ProducerTopicMessage(txnUuid = UUIDs.timeBased(), ttl = 10, status = ProducerTransactionStatus.update, partition = 1)
    var cnt = 0
    val mf = (m: ProducerTopicMessage) => { cnt +=1 }
    cm.addCallback(mf)
    cm.invokeCallbacks(m)
    cnt shouldBe 1
  }
}
