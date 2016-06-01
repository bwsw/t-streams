package com.bwsw.tstreams.policy

import com.bwsw.tstreams.streams.BasicStream

/**
 * Round robin policy impl of [[AbstractPolicy]]]
 * @param usedPartitions Partitions from which agent will interact
 */

class RoundRobinPolicy(stream : BasicStream[_], usedPartitions : List[Int])
  extends AbstractPolicy(usedPartitions = usedPartitions, stream = stream){

  /**
   * Get next partition to interact and update round value
   * @return Next partition
   */
  override def getNextPartition: Int = {
    val partition = usedPartitions(currentPos)

    if (roundPos < usedPartitions.size)
      roundPos += 1

    currentPos += 1
    currentPos %= usedPartitions.size

    partition
  }
}



