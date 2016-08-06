package agents.subscriber

import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import testutils.TestUtils

/**
  * Created by ivan on 06.08.16.
  */
class SubscriberManagerTests  extends FlatSpec with Matchers with BeforeAndAfterAll with TestUtils{
  override def afterAll(): Unit = {
    onAfterAll()
  }
}
