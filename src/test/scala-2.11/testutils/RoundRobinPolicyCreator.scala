package testutils

import com.bwsw.tstreams.policy.RoundRobinPolicy
import com.bwsw.tstreams.streams.BasicStream

/**
  * Repo for creating some defined policies
  */
object RoundRobinPolicyCreator {
  /**
    *
    * @param stream         Stream instance
    * @param usedPartitions Policy partitions to use
    * @return RoundRobinPolicy instance
    */
  def getRoundRobinPolicy(stream: BasicStream[_], usedPartitions: List[Int]): RoundRobinPolicy =
    new RoundRobinPolicy(stream, usedPartitions)
}