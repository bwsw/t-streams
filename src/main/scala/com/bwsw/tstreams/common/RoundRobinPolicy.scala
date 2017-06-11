package com.bwsw.tstreams.common

/**
  * Round robin policy impl of [[AbstractPolicy]]]
  *
  * @param usedPartitions Partitions from which agent will interact
  */

class RoundRobinPolicy(partitionsCount: Int, usedPartitions: Set[Int])
  extends AbstractPolicy(partitionsCount, usedPartitions = usedPartitions) {

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



