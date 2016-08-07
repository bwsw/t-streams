package agents.subscriber

import com.bwsw.tstreams.coordination.messages.state.{Message, TransactionStatus}
import com.bwsw.tstreams.coordination.subscriber.CallbackManager
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
    val m = Message(txnUuid = UUIDs.timeBased(), ttl = 10, status = TransactionStatus.update, partition = 1)
    var cnt = 0
    val mf = (m: Message) => { cnt +=1 }
    cm.addCallback(mf)
    cm.invokeCallbacks(m)
    cnt shouldBe 1
  }
}
