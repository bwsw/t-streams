package ut

import com.bwsw.tstreamstransactionserver.netty.server.OrderedExecutionContextPool
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

class OrderedExecutionContextPoolTest
  extends FlatSpec
    with Matchers
{

  private final class RandomStreamIDAndItsPartition(streamIdMax: Int,
                                                    partitionMax: Int
                                                   )
  {
    private val rand = Random
    def generate: (Int, Int) = {
      val streamID = rand.nextInt(streamIdMax)
      val partition = rand.nextInt(partitionMax)
      (streamID, partition)
    }

  }


  private def calculatePoolIndex(stream: Int, partition: Int, n: Int) = {
    ((stream % n) + (partition % n)) % n
  }


  "Ordered execution pool" should "should assign tasks as formula requires if pool size is even" in {
    val poolSize = 6
    val orderedExecutionPool = new OrderedExecutionContextPool(poolSize)

    val indexToContext = collection.mutable.Map[Int, String]()

    val rand = new RandomStreamIDAndItsPartition(10000, 10000)
    (0 to 1000).foreach { _ =>
      val (streamID, partition) = rand.generate
      val index = calculatePoolIndex(streamID, partition, poolSize)
      indexToContext.get(index)
        .map(contextName =>
          contextName shouldBe orderedExecutionPool.pool(streamID, partition).toString
        )
        .orElse(
          indexToContext.put(index, orderedExecutionPool.pool(streamID, partition).toString)
        )
    }

    orderedExecutionPool.close()
  }

  it should "should assign tasks as formula requires if pool size is odd" in {
    val poolSize = 13
    val orderedExecutionPool = new OrderedExecutionContextPool(poolSize)

    val indexToContext = collection.mutable.Map[Int, String]()

    val rand = new RandomStreamIDAndItsPartition(10000, 10000)
    (0 to 1000).foreach { _ =>
      val (streamID, partition) = rand.generate
      val index = calculatePoolIndex(streamID, partition, poolSize)
      indexToContext.get(index)
        .map(contextName =>
          contextName shouldBe orderedExecutionPool.pool(streamID, partition).toString
        )
        .orElse(
          indexToContext.put(index, orderedExecutionPool.pool(streamID, partition).toString)
        )
    }

    orderedExecutionPool.close()
  }



}
