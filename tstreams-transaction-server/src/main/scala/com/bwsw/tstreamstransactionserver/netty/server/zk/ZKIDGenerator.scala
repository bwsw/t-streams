package com.bwsw.tstreamstransactionserver.netty.server.zk

import com.bwsw.commitlog.IDGenerator
import org.apache.curator.RetryPolicy
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong

import scala.annotation.tailrec

final class ZKIDGenerator(curatorClient: CuratorFramework,
                          retryPolicy: RetryPolicy,
                          path: String)
  extends IDGenerator[Long] {

  private val distributedAtomicLong =
    new DistributedAtomicLong(
      curatorClient,
      path,
      retryPolicy
    )

  distributedAtomicLong.initialize(-1L)

  override def nextID: Long = {
    val operation = distributedAtomicLong.increment()
    if (operation.succeeded()) {
      val newID = operation.postValue()
      newID
    }
    else
      throw new Exception(
        s"Can't increment counter by 1: " +
          s"previous was ${operation.preValue()} " +
          s"but now it's ${operation.postValue()}."
      )
  }

  @tailrec
  override def currentID: Long = {
    val operation = distributedAtomicLong.get()
    if (operation.succeeded()) {
      operation.postValue()
    } else {
      currentID
    }
  }

  def setID(id: Long): Long = {
    distributedAtomicLong
      .trySet(id)
      .postValue()
  }
}
