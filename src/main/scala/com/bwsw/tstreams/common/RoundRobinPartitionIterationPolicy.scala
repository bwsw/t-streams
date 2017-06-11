package com.bwsw.tstreams.common

/**
  * Round robin policy impl of [[PartitionIterationPolicy]]]
  *
  * @param usedPartitions Partitions from which agent will interact
  */

class RoundRobinPartitionIterationPolicy(partitionsCount: Int, usedPartitions: Set[Int])
  extends PartitionIterationPolicy(partitionsCount, usedPartitions = usedPartitions) {

  /**
    * Get next partition to interact and update round value
    *
    * @return Next partition
    */
  override def getNextPartition(): Int = this.synchronized {
    if(currentPos == usedPartitionsList.size) startNewRound()
    val partition = usedPartitionsList(currentPos)
    currentPos += 1
    partition
  }
}



