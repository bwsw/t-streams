package com.bwsw.tstreams.agents.producer

import java.util.concurrent.ConcurrentHashMap

import com.bwsw.tstreams.common.ResettableCountDownLatch

/**
  * Created by Ivan Kudryavtsev on 28.08.16.
  * Used to protect early materialization operations while transaction is not fully created yet
  */
class MaterializationGovernor(partitions: Set[Int]) {
  private val transactionMaterializationBlockingMap = new ConcurrentHashMap[Int, ResettableCountDownLatch]()
  partitions.map(p => transactionMaterializationBlockingMap.put(p, new ResettableCountDownLatch(0)))

  /**
    * protects from operations
    * @param partition
    */
  def protect(partition: Int)          = transactionMaterializationBlockingMap.get(partition).setValue(1)

  /**
    * Disables the protection
    * @param partition
    */
  def unprotect(partition: Int)        = transactionMaterializationBlockingMap.get(partition).countDown()

  /**
    * Waits until the protection is unset
    * @param partition
    */
  def awaitUnprotected(partition: Int) = transactionMaterializationBlockingMap.get(partition).await()

}
