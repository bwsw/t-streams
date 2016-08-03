package com.bwsw.tstreams.common

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

import org.slf4j.Logger

/**
  * Created by ivan on 02.08.16.
  */
object LockUtil {
  def lockOrDie(l: ReentrantLock, lt: (Int, TimeUnit), logger: Option[Logger] = None): Unit = {
    if(!l.tryLock(lt._1, lt._2))
    {
      if (logger.isDefined)
        logger.get.error(s"Failed to get lock object ${l.toString} in ${lt._1} ${lt._2.toString}.")
      throw new IllegalStateException(s"Failed to get lock object in ${lt._1} ${lt._2.toString}.")
    } else {
      if (logger.isDefined)
        logger.get.debug(s"Lock object ${l.toString} received.")
    }
  }

  def withLockOrDieDo[RTYPE](l: ReentrantLock,
                        lt: (Int, TimeUnit),
                        logger: Option[Logger] = None,
                        lambda: () => RTYPE): RTYPE = {
    LockUtil.lockOrDie(l, lt, logger)
    try {
      val rv = lambda()
      l.unlock()
      if (logger.isDefined)
        logger.get.debug(s"Unlocked ${l.toString} in ${lt._1} ${lt._2.toString}.")
      return rv
    } catch {
      case e: Exception =>
        l.unlock()
        throw e
    }
  }
}
