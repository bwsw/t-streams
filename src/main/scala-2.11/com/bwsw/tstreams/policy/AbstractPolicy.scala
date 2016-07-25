package com.bwsw.tstreams.policy

import com.bwsw.tstreams.streams.BasicStream

/**
  * Basic interface for policies
  * Class is not thread safe. User must create separate instance for every producer/consumer
  */
abstract class AbstractPolicy(stream : BasicStream[_], usedPartitions : List[Int]){

  /**
   * Partitions validation
   */
  if(usedPartitions.isEmpty)
    throw new IllegalArgumentException("usedPartitions can't be empty")

  usedPartitions.foreach{x=>
    if(x < 0 || x >= stream.getPartitions)
      throw new IllegalArgumentException(s"invalid partition:{$x} in usedPartitions")
  }


  /**
   * Used by classes that implement policy logic to determine current partition
   */
  protected var currentPos = 0

  /**
   * @return Next partition (start from the first partition of usedPartitions)
   */
  def getNextPartition : Int

  /**
   * @return Current partition
   */
  def getCurrentPartition : Int = usedPartitions(currentPos)

  /**
   * Used by starting new round from though all usedPartitions
   */
  protected var roundPos : Int = 0

  /**
   * Starting new round
   */
  def startNewRound() : Unit = roundPos = 0

  /**
   *
   * @return Finished round or not
   */
  def isRoundFinished() : Boolean = roundPos >= usedPartitions.size

  /**
   * Getter for used partitions
   * @return Used partitions
   */
  def getUsedPartition() : List[Int] = usedPartitions
}
