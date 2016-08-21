package agents.subscriber

import java.util.UUID

import com.bwsw.tstreams.agents.consumer.subscriber_v2.{TransactionFastLoader, TransactionState}
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable

trait FastLoaderTestContainer {
  val lastTransactionsMap = mutable.Map[Int, TransactionState]()
  val fastLoader = new TransactionFastLoader(partitions(), lastTransactionsMap)

  def partitions() = Set(0)

  def init()
  def test()
}

/**
  * Created by ivan on 21.08.16.
  */
class TransactionFastLoaderTests extends FlatSpec with Matchers {

  it should ""
}
