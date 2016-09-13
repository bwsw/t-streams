package com.bwsw.tstreams.common

import com.bwsw.tstreams.streams.TStream

/**
  * Round robin policy impl of [[AbstractPolicy]]]
  *
  * @param usedPartitions Partitions from which agent will interact
  */

class RoundRobinPolicy(stream: TStream[_], usedPartitions: Set[Int])
  extends AbstractPolicy(stream = stream, usedPartitions = usedPartitions) {

  /**
    * Get next partition to interact and update round value
    *
    * @return Next partition
    */
  override def getNextPartition(): Int = this.synchronized {
    val partition = usedPartitionsList(currentPos)

    if (roundPos < usedPartitionsList.size)
      roundPos += 1

    currentPos += 1
    currentPos %= usedPartitionsList.size

    partition
  }
}



