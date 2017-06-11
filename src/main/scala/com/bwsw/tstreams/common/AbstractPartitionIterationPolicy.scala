package com.bwsw.tstreams.common

/**
  * Basic interface for policies
  */
abstract class AbstractPartitionIterationPolicy(partitionsCount: Int, usedPartitions: Set[Int]) {

  protected val usedPartitionsList = usedPartitions.toList
  @volatile protected var currentPos = 0

  if (usedPartitionsList.isEmpty)
    throw new IllegalArgumentException("UsedPartitions can't be empty")

  if(!usedPartitionsList.forall(partition => partition >= 0 && partition < partitionsCount))
    throw new IllegalArgumentException(s"Invalid partition found among $usedPartitionsList")

  def getNextPartition: Int

  protected[tstreams] def getCurrentPartition: Int = this.synchronized {
    usedPartitionsList(currentPos)
  }

  protected[tstreams] def startNewRound() = {
    currentPos = 0
  }

  def getUsedPartitions = usedPartitions
}
