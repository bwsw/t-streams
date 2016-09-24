package com.bwsw.tstreams.common

import com.bwsw.tstreams.streams.Stream

/**
  * Basic interface for policies
  * Class is not thread safe. User must create separate instance for every producer/consumer
  */
abstract class AbstractPolicy(stream: Stream[_], usedPartitions: Set[Int]) {

  protected val usedPartitionsList = usedPartitions.toList

  /**
    * Used by classes that implement policy logic to determine current partition
    */
  protected var currentPos = 0

  /**
    * Used by starting new round from though all usedPartitions
    */
  protected var roundPos: Int = 0

  /**
    * Partitions validation
    */
  if (usedPartitionsList.isEmpty)
    throw new IllegalArgumentException("UsedPartitions can't be empty")

  usedPartitionsList.foreach { x =>
    if (x < 0 || x >= stream.getPartitions)
      throw new IllegalArgumentException(s"Invalid partition:{$x} in usedPartitions")
  }

  /**
    * @return Next partition (start from the first partition of usedPartitions)
    */
  def getNextPartition(): Int

  /**
    * @return Current partition
    */
  def getCurrentPartition(): Int = this.synchronized {
    usedPartitionsList(currentPos)
  }


  /**
    * Starting new round
    */
  def startNewRound(): Unit = this.synchronized {
    roundPos = 0
  }

  /**
    *
    * @return Finished round or not
    */
  def isRoundFinished(): Boolean = this.synchronized {
    roundPos >= usedPartitionsList.size
  }

  /**
    * Getter for used partitions
    *
    * @return Used partitions
    */
  def getUsedPartitions(): List[Int] = this.synchronized {
    usedPartitionsList
  }
}
